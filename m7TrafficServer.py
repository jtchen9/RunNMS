"""Owned Windows iperf3 server pool; no changes to robot results or AP reports."""
import atexit
import ctypes
import ipaddress
import json
import os
from pathlib import Path
import socket
import subprocess
import threading
import time
from datetime import datetime

import config
from fastapi import APIRouter

router = APIRouter()
_NO_WINDOW = getattr(subprocess, 'CREATE_NO_WINDOW', 0)


def _run(command):
    return subprocess.run(command, capture_output=True, text=True, encoding='utf-8',
                          errors='replace', timeout=10, check=True, creationflags=_NO_WINDOW).stdout


def select_address(addresses, requested='', excluded='192.168.11.0/24'):
    usable = []
    for text in addresses:
        try:
            address = ipaddress.IPv4Address(str(text))
        except ipaddress.AddressValueError:
            continue
        if address.is_loopback or address.is_link_local or address.is_unspecified or address.is_multicast:
            continue
        if excluded and address in ipaddress.IPv4Network(excluded):
            continue
        usable.append(str(address))
    usable = sorted(set(usable))
    if requested:
        if requested not in usable:
            raise RuntimeError('Configured bind address is not usable on the selected Ethernet interface')
        return requested
    if len(usable) != 1:
        raise RuntimeError('Expected one upstream Ethernet IPv4 address; found ' + repr(usable))
    return usable[0]


def discover_address():
    alias = str(config.TRAFFIC_LOCAL_INTERFACE_ALIAS).replace("'", "''")
    script = ("[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; "
              "@(Get-NetIPAddress -InterfaceAlias '" + alias + "' -AddressFamily IPv4 -ErrorAction Stop | "
              "Where-Object {$_.AddressState -eq 'Preferred'} | Select-Object -ExpandProperty IPAddress) | ConvertTo-Json -Compress")
    result = json.loads(_run(['powershell.exe', '-NoProfile', '-NonInteractive', '-Command', script]).lstrip('\ufeff') or '[]')
    return select_address(result if isinstance(result, list) else [result],
                          str(config.TRAFFIC_LOCAL_BIND_IP).strip(), config.TRAFFIC_LOCAL_EXCLUDED_SUBNET)


def listener_owners():
    """Inspect listeners without opening control connections that disturb tests."""
    owners = {}
    for line in _run(['netstat.exe', '-ano', '-p', 'tcp']).splitlines():
        fields = line.split()
        if len(fields) != 5 or fields[0] != 'TCP' or fields[2] not in ('0.0.0.0:0', '[::]:0'):
            continue
        try:
            host, port = fields[1].rsplit(':', 1)
            owners[(host.strip('[]'), int(port))] = int(fields[4])
        except (ValueError, IndexError):
            continue
    return owners


class WindowsJob:
    """Close-on-owner-exit cleanup, including VS Code force-stop and child processes."""
    def __init__(self):
        from ctypes import wintypes as w
        class Basic(ctypes.Structure):
            _fields_ = [('PerProcessUserTimeLimit', ctypes.c_int64), ('PerJobUserTimeLimit', ctypes.c_int64),
                        ('LimitFlags', w.DWORD), ('MinimumWorkingSetSize', ctypes.c_size_t),
                        ('MaximumWorkingSetSize', ctypes.c_size_t), ('ActiveProcessLimit', w.DWORD),
                        ('Affinity', ctypes.c_size_t), ('PriorityClass', w.DWORD), ('SchedulingClass', w.DWORD)]
        class IO(ctypes.Structure):
            _fields_ = [(name, ctypes.c_uint64) for name in ('ReadOperationCount', 'WriteOperationCount',
                        'OtherOperationCount', 'ReadTransferCount', 'WriteTransferCount', 'OtherTransferCount')]
        class Extended(ctypes.Structure):
            _fields_ = [('BasicLimitInformation', Basic), ('IoInfo', IO), ('ProcessMemoryLimit', ctypes.c_size_t),
                        ('JobMemoryLimit', ctypes.c_size_t), ('PeakProcessMemoryUsed', ctypes.c_size_t),
                        ('PeakJobMemoryUsed', ctypes.c_size_t)]
        self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        self.kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, w.LPCWSTR]
        self.kernel.CreateJobObjectW.restype = w.HANDLE
        self.kernel.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]
        self.kernel.SetInformationJobObject.restype = w.BOOL
        self.kernel.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
        self.kernel.AssignProcessToJobObject.restype = w.BOOL
        self.kernel.CloseHandle.argtypes = [w.HANDLE]
        self.kernel.CloseHandle.restype = w.BOOL
        self.handle = self.kernel.CreateJobObjectW(None, None)
        if not self.handle:
            raise ctypes.WinError(ctypes.get_last_error())
        limits = Extended()
        limits.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        if not self.kernel.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
            error = ctypes.WinError(ctypes.get_last_error())
            self.close()
            raise error

    def attach(self, proc):
        if not self.kernel.AssignProcessToJobObject(self.handle, int(proc._handle)):
            raise ctypes.WinError(ctypes.get_last_error())

    def close(self):
        if self.handle:
            self.kernel.CloseHandle(self.handle)
            self.handle = None


def validated_ports(ports):
    if not ports or len(ports) > 100 or len(set(ports)) != len(ports) or any(type(p) is not int or not 1 <= p <= 65535 for p in ports):
        raise ValueError('Use 1..100 distinct integer server ports')
    return list(ports)


class PortLog:
    """Drain each child continuously; create its file only on first output."""
    def __init__(self, path):
        self.path = Path(path)
        self.thread = None
        self.closed = False
        self.error = ''

    def start(self, stream):
        self.thread = threading.Thread(target=self._copy, args=(stream,),
                                       name='iperf-log-' + self.path.stem, daemon=True)
        self.thread.start()

    def _copy(self, stream):
        output = None
        try:
            read = getattr(stream, 'read1', stream.read)
            while True:
                chunk = read(65536)
                if not chunk:
                    break
                if output is None:
                    output = self.path.open('ab', buffering=0)
                output.write(chunk)
        except Exception as exc:
            self.error = str(exc)
            print('[IPERF3 LOG ERROR]', self.path, exc, flush=True)
        finally:
            if output is not None:
                output.close()
            stream.close()
            self.closed = True

    def close(self):
        if self.thread:
            self.thread.join(timeout=3)
            if self.thread.is_alive():
                self.error = 'log drain pending after server shutdown'
        else:
            self.closed = True


class Pool:
    def __init__(self):
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.thread = None
        self.guard = None
        self.job = None
        self.processes = {}
        self.host = ''
        self.ready = []
        self.errors = {}
        self.error = ''
        self.log_dir = None
        self.updated_at = None

    def start(self):
        with self.lock:
            if self.thread and self.thread.is_alive():
                return
            if not config.TRAFFIC_LOCAL_SERVER_ENABLED:
                return
            self.stop_event.clear()
            self.thread = threading.Thread(target=self._run, name='iperf3-server-pool', daemon=True)
            self.thread.start()

    def _run(self):
        try:
            if os.name != 'nt':
                raise RuntimeError('Local server manager requires Windows; disable it for an external server')
            validated_ports(config.TRAFFIC_SERVER_PORTS)
            exe = Path(config.TRAFFIC_LOCAL_IPERF3_BIN)
            if not exe.is_file():
                raise RuntimeError('iperf3 executable not found: ' + str(exe))
            self.guard = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # No SO_REUSEADDR: a second NMS cannot silently own the same pool.
            self.guard.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            self.guard.bind(('127.0.0.1', config.TRAFFIC_LOCAL_LOCK_PORT))
            self.guard.listen(1)
            self.job = WindowsJob()
            self.log_dir = Path(config.TRAFFIC_LOCAL_LOG_DIR) / (datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + str(os.getpid()))
            self.log_dir.mkdir(parents=True, exist_ok=True)
            while not self.stop_event.is_set():
                try:
                    self.tick()
                except Exception as exc:
                    with self.lock:
                        self.ready = []
                        self.error = str(exc)
                self.stop_event.wait(config.TRAFFIC_LOCAL_CHECK_SECONDS)
        except Exception as exc:
            with self.lock:
                self.error = str(exc)
            print('[IPERF3 SERVER ERROR]', exc, flush=True)
        finally:
            with self.lock:
                self.ready = []
                if self.job:
                    self.job.close()
                    self.job = None
                self._stop_children()
                if self.guard:
                    self.guard.close()
                    self.guard = None

    def tick(self):
        host = discover_address()
        owners = listener_owners()
        with self.lock:
            self.ready = []
            if self.host and self.host != host:
                print('[IPERF3 SERVER] Ethernet address changed; restarting pool. Active tests may fail.', flush=True)
                self._stop_children()
            self.host = host
            for port in config.TRAFFIC_SERVER_PORTS:
                entry = self.processes.get(port)
                if entry and entry[0].poll() is not None:
                    self.errors[str(port)] = 'server exited with code ' + str(entry[0].returncode)
                    entry[1].close()
                    del self.processes[port]
                    entry = None
                if entry is None:
                    # Do not take over or kill existing manual/unrelated listeners.
                    if any(p == port and h in (host, '0.0.0.0', '::') for h, p in owners):
                        self.errors[str(port)] = 'port already occupied; stop manual server or choose another port'
                        continue
                    log = PortLog(self.log_dir / ('port-' + str(port) + '.jsonl'))
                    proc = None
                    try:
                        cmd = [str(config.TRAFFIC_LOCAL_IPERF3_BIN), '-s', '-B', host, '-p', str(port),
                               '-i', '60', '--json-stream', '--json-stream-full', '--forceflush']
                        proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                                creationflags=_NO_WINDOW, close_fds=True)
                        self.job.attach(proc)
                        log.start(proc.stdout)
                        self.processes[port] = (proc, log)
                        self.errors.pop(str(port), None)
                    except Exception as exc:
                        if proc:
                            proc.kill()
                            proc.wait(timeout=3)
                            if proc.stdout is not None and log.thread is None:
                                proc.stdout.close()
                        log.close()
                        self.errors[str(port)] = str(exc)
                    continue  # Readiness requires the next listener-owner observation.
                if owners.get((host, port)) == entry[0].pid:
                    self.ready.append(port)
                    self.errors.pop(str(port), None)
                else:
                    self.errors[str(port)] = 'waiting for owned listener'
            self.updated_at = datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
            self.error = ''

    def _stop_children(self):
        entries = list(self.processes.values())
        self.processes.clear()
        # Signal every child before waiting, so 40 ports do not multiply timeouts.
        for proc, log in entries:
            try:
                if proc.poll() is None:
                    proc.terminate()
            except OSError:
                pass
        deadline = time.monotonic() + 3
        for proc, log in entries:
            try:
                if proc.poll() is None:
                    try:
                        proc.wait(timeout=max(0, deadline - time.monotonic()))
                    except subprocess.TimeoutExpired:
                        proc.kill()
            except OSError:
                pass
            finally:
                log.close()

    def stop(self):
        self.stop_event.set()
        thread = self.thread
        if thread:
            thread.join(timeout=35)
            if thread.is_alive():
                raise RuntimeError('iperf3 manager did not finish shutdown')

    def endpoint(self):
        with self.lock:
            # Readiness is a listener check, not a reservation or a guarantee it is idle.
            live = [p for p in self.ready if p in self.processes and self.processes[p][0].poll() is None]
            if self.error or not self.host or not live or self.stop_event.is_set():
                raise RuntimeError('Local iperf3 server pool unavailable: ' + (self.error or 'no verified listeners yet'))
            return self.host, live

    def status(self):
        with self.lock:
            return {'enabled': bool(config.TRAFFIC_LOCAL_SERVER_ENABLED), 'host': self.host,
                    'configured_ports': list(config.TRAFFIC_SERVER_PORTS), 'ready_ports': list(self.ready),
                    'ready_count': len(self.ready),
                    'errors': {**self.errors, **{str(p): 'log: ' + log.error
                               for p, (proc, log) in self.processes.items() if log.error}},
                    'error': self.error,
                    'updated_at': self.updated_at, 'log_dir': str(self.log_dir) if self.log_dir else None,
                    'running': bool(self.thread and self.thread.is_alive())}


pool = Pool()
atexit.register(pool.stop)


@router.get('/traffic/_server_status', tags=['7 iperf3 Traffic'])
def server_status():
    return pool.status()
