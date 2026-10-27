"""Opt-in robot client delivery; Redis owns handoff and report deduplication."""
import hashlib
import json
import re

import config
import utility


def _key(kind, value=''):
    return config.KEY_PREFIX + 'traffic:remote:' + kind + ':' + value


def remote_enabled(scanner):
    return bool(str(getattr(config, 'TRAFFIC_SERVER_HOST', '')).strip()) and scanner in getattr(config, 'TRAFFIC_REMOTE_SCANNERS', [])


def has_remote_session(scanner, fields):
    args = json.loads(fields.get('args_json') or '{}')
    state = config.r.get(config.key_traffic_temp_running(scanner, str(args.get('session_id') or '')))
    return bool(state and json.loads(state).get('remote'))


# Handoff must survive a worker retry or a lost Redis response without enqueueing
# the same robot command twice. No application-level check-then-write race.
_HANDOFF = '''
local prior = redis.call('GET', KEYS[3])
if prior then
    redis.call('XDEL', KEYS[1], ARGV[1])
    return prior
end
if #redis.call('XRANGE', KEYS[1], ARGV[1], ARGV[1]) == 0 then return 'cancelled' end
if ARGV[2] ~= '' then
    local active = redis.call('GET', KEYS[5])
    if active and cjson.decode(active).execution_id ~= cjson.decode(ARGV[2]).execution_id then
        return redis.error_reply('traffic session already active')
    end
end
local id = redis.call('XADD', KEYS[2], '*', unpack(ARGV, 4))
redis.call('SET', KEYS[3], id)
if ARGV[2] ~= '' then redis.call('SET', KEYS[4], ARGV[2]) end
if ARGV[3] ~= '' then redis.call('SET', KEYS[5], ARGV[3]) end
redis.call('XDEL', KEYS[1], ARGV[1])
return id
'''


def dispatch_due(xid, fields):
    scanner = fields['scanner']
    action = fields['action']
    args = json.loads(fields.get('args_json') or '{}')
    session = str(args.get('session_id') or '').strip()
    if not session:
        raise ValueError('session_id missing')
    running_key = config.key_traffic_temp_running(scanner, session)
    context_json = active_json = ''
    if action == 'traffic.session.start':
        ports = list(getattr(config, 'TRAFFIC_SERVER_PORTS', []))
        if not ports or len(ports) > 100 or any(type(p) is not int or not 1 <= p <= 65535 for p in ports):
            raise ValueError('TRAFFIC_SERVER_PORTS must contain 1..100 valid ports')
        # Start at different ports for concurrent jobs. Server admission plus
        # busy-only retries arbitrate public ports; this is not a reservation.
        offset = (int(config.r.incr(_key('port_cursor'))) - 1) % len(ports)
        ports = ports[offset:] + ports[:offset]
        args.update(server_host=str(config.TRAFFIC_SERVER_HOST).strip(),
                    server_port=ports[0], server_ports=ports)
        execution = 'remote-' + hashlib.sha256((scanner + ':' + xid).encode()).hexdigest()
        context = {'scanner': scanner, 'session_id': session, 'execution_id': execution,
                   'lab_id': fields.get('lab_id', ''), 'experiment_id': fields.get('experiment_id', ''),
                   'experiment_session_id': fields.get('session_id', ''),
                   'args': args, 'created_at': utility.local_ts()}
        context_json = json.dumps(context)
        active_json = json.dumps({'remote': True, 'execution_id': execution})
    elif action == 'traffic.session.stop':
        state = json.loads(config.r.get(running_key) or '{}')
        execution = str(state.get('execution_id') or '')
        if not state.get('remote'):
            # Cancel a future start that has not yet been handed to the robot.
            ids = []
            for queued_id, queued in config.r.xrange(config.KEY_TRAFFIC_CMD_STREAM, count=5000):
                queued_args = json.loads(queued.get('args_json') or '{}')
                if queued.get('scanner') == scanner and queued.get('action') == 'traffic.session.start' and queued_args.get('session_id') == session:
                    ids.append(queued_id)
            from m7Traffic import _push_event
            _push_event(scanner, session, action, 'ok' if ids else 'error',
                        'cancelled queued start' if ids else 'no remote session found')
            config.r.xdel(config.KEY_TRAFFIC_CMD_STREAM, xid, *ids)
            return
        args['execution_id'] = execution
        args['stop_command_id'] = xid
    else:
        raise ValueError('unsupported traffic action')
    delivery = {'category': 'traffic', 'action': action,
                'execute_at': fields['execute_at'], 'created_at': utility.local_ts(),
                'args_json': json.dumps(args), 'traffic_execution_id': execution,
                'lab_id': fields.get('lab_id', ''), 'experiment_id': fields.get('experiment_id', ''),
                'session_id': fields.get('session_id', '')}
    flat = [str(v) for pair in delivery.items() for v in pair]
    config.r.eval(_HANDOFF, 5, config.KEY_TRAFFIC_CMD_STREAM, config.key_cmd_stream(scanner),
                  _key('handoff', xid), _key('execution', execution), running_key,
                  xid, context_json, active_json, *flat)


_ACCEPT = '''
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local target_type = redis.call('TYPE', KEYS[2]).ok
local debug_type = redis.call('TYPE', KEYS[3]).ok
local active_type = redis.call('TYPE', KEYS[4]).ok
local snapshot_type = redis.call('TYPE', KEYS[5]).ok
if (target_type ~= 'none' and target_type ~= 'stream') or
   (ARGV[1] == 'event' and debug_type ~= 'none' and debug_type ~= 'stream') or
   (active_type ~= 'none' and active_type ~= 'string') or
   (snapshot_type ~= 'none' and snapshot_type ~= 'string') then
    return redis.error_reply('traffic storage key has wrong type')
end
redis.call('XADD', KEYS[2], '*', unpack(ARGV, 5))
if ARGV[1] == 'event' then
    redis.call('XADD', KEYS[3], 'MAXLEN', '~', ARGV[2], '*', unpack(ARGV, 5))
end
redis.call('SET', KEYS[1], '1')
if ARGV[1] == 'result' then
    redis.call('SET', KEYS[5], ARGV[4], 'EX', 172800)
    local current = redis.call('GET', KEYS[4])
    if current then
        local state = cjson.decode(current)
        if state.execution_id == ARGV[3] then redis.call('DEL', KEYS[4]) end
    end
end
return 1
'''


def ingest_records(scanner, records):
    """ACK only after atomic storage. Repeated IDs are safe after a lost reply.

    Unknown old/manual records are explicitly rejected for local quarantine,
    not uploaded against whatever experiment happens to be current.
    """
    accepted, rejected = [], []
    for record in (records or [])[:20]:
        record_id = record.get('record_id')
        if not isinstance(record_id, str) or not re.fullmatch(r'[0-9a-f]{32}', record_id):
            continue
        try:
            if record.get('scanner') != scanner:
                raise ValueError('scanner mismatch')
            execution = str(record.get('execution_id') or '')
            kind = record.get('kind')
            if kind not in ('event', 'result'):
                raise ValueError('unsupported traffic record kind')
            context = json.loads(config.r.get(_key('execution', execution)) or '{}')
            if context.get('scanner') != scanner or context.get('session_id') != record.get('session_id'):
                raise ValueError('unknown execution (manual/old records retained in robot quarantine)')
            status = record.get('status')
            if status not in ('ok', 'error', 'stopped'):
                raise ValueError('invalid traffic status')
            if not isinstance(record.get('raw', {}), dict):
                raise ValueError('raw must be a JSON object')
            # Shape and scalar values remain compatible with m5Northbound.
            values = {'scanner': scanner, 'session_id': context['session_id'],
                      'execution_id': execution, 'record_id': record_id,
                      'completion_time': record.get('completion_time') or utility.local_ts(),
                      'status': status, 'detail': record.get('detail') or '',
                      'lab_id': context['lab_id'], 'experiment_id': context['experiment_id'],
                      'experiment_session_id': context['experiment_session_id'],
                      'client_role': 'robot'}
            for name in ('protocol', 'ac', 'bitrate', 'packet_size', 'parallel', 'duration_sec',
                         'started_at', 'server_host', 'server_port'):
                value = record.get(name, context['args'].get(name))
                if value is not None:
                    values[name] = value
            reverse = context['args'].get('reverse')
            if type(reverse) is not bool:
                raise ValueError('stored reverse must be boolean')
            values['reverse'] = '1' if reverse else '0'
            values['iperf_reverse'] = '0' if reverse else '1'
            for name in ('wifi_start', 'wifi_end'):
                if isinstance(record.get(name), dict):
                    values[name + '_json'] = json.dumps(record[name])
            if kind == 'result':
                values['raw_json'] = json.dumps(record.get('raw', {}), ensure_ascii=False)
            else:
                if record.get('action') not in ('traffic.session.start', 'traffic.session.stop'):
                    raise ValueError('unsupported traffic event action')
                values['action'] = record['action']
            flat = [str(v) for pair in values.items() for v in pair]
            target = config.KEY_TRAFFIC_RESULT_STREAM if kind == 'result' else config.KEY_TRAFFIC_EVENT_STREAM
            snapshot = {**record, 'lab_id': context['lab_id'], 'experiment_id': context['experiment_id'],
                        'experiment_session_id': context['experiment_session_id']}
            config.r.eval(_ACCEPT, 5, _key('accepted', scanner + ':' + record_id), target,
                          config.KEY_TRAFFIC_EVENT_TEMP_STREAM,
                          config.key_traffic_temp_running(scanner, context['session_id']), _key('last_result', scanner),
                          kind, config.TRAFFIC_EVENT_TEMP_MAXLEN, execution, json.dumps(snapshot), *flat)
            accepted.append(record_id)
        except ValueError as exc:
            rejected.append({'record_id': record_id, 'detail': str(exc)})
        except Exception:
            # Redis/storage failures are retryable: never ACK or quarantine.
            continue
    return accepted, rejected


def latest_result(scanner):
    return json.loads(config.r.get(_key('last_result', scanner)) or '{}')
