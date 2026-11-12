"""Small Swagger-only enhancement for the multipart experiment-registration form."""
from datetime import timedelta
from html import escape
from fastapi import Request
from starlette.responses import Response, JSONResponse
import config
import utility


def suggested_t0(now):
    # Ceiling of (now + 60 seconds) to a whole minute, including midnight rollover.
    minute = now.replace(second=0, microsecond=0)
    return minute + timedelta(minutes=1 if now == minute else 2)


_JS = r"""
(() => {
  const clock = new URL('t0', document.currentScript.src);
  const active = new WeakMap();
  document.addEventListener('click', async ev => {
    const button = ev.target.closest('button.try-out__btn');
    const op = button && button.closest('.opblock');
    if (!op || !(op.querySelector('.opblock-summary-path')?.textContent || '')
        .trim().endsWith('/cmd/_load_csv_file')) return;
    const pending = active.get(op);
    if (pending) pending.abort();
    if (button.classList.contains('cancel') || button.textContent.trim() === 'Cancel') return;
    const task = new AbortController();
    active.set(op, task);
    const field = () => op.querySelector('input[placeholder="t0"]') ||
      [...op.querySelectorAll('tr')].find(r =>
        r.querySelector('.parameter__name')?.firstChild?.textContent.trim() === 't0'
      )?.querySelector('input[type=text]');
    const manual = e => { if (e.target === field() && e.isTrusted) task.abort(); };
    op.addEventListener('input', manual, true);
    try {
      const response = await fetch(clock, {cache:'no-store', signal:task.signal});
      if (!response.ok) throw Error('HTTP ' + response.status);
      const {t0} = await response.json();
      for (let i=0; i<40 && !task.signal.aborted; i++) {
        const el = field();
        if (el && !el.disabled && !el.readOnly) {
          // Use the native setter so React receives the new form value on input.
          Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(el,t0);
          el.dispatchEvent(new Event('input',{bubbles:true}));
          break;
        }
        await new Promise(r => setTimeout(r,25));
      }
    } catch (e) { if (!task.signal.aborted) console.warn('NMS start time unavailable:',e); }
    finally { op.removeEventListener('input',manual,true); }
  }, true);
})();
"""


def install_registration_swagger(app):
    if not app.docs_url:
        return
    docs = app.docs_url
    prefix = docs.rstrip('/') + '/_registration'

    @app.get(prefix + '/t0', include_in_schema=False)
    def registration_clock():
        now = utility.parse_local_dt(utility.local_ts())
        return JSONResponse({'t0': suggested_t0(now).strftime(config.TIME_FMT)},
                            headers={'Cache-Control': 'no-store'})

    @app.get(prefix + '/helper.js', include_in_schema=False)
    def registration_swagger_js():
        return Response(_JS, media_type='application/javascript')

    # Customize only the existing Swagger HTML; keep its original settings,
    # OAuth redirect and all API request/response handling intact.
    from starlette.routing import Route, request_response
    route = next(r for r in app.routes if isinstance(r, Route) and r.path == docs)
    original_docs = route.endpoint

    async def with_helper(request: Request):
        html = await original_docs(request)
        src = escape(str(request.url_for('registration_swagger_js').path), quote=True)
        html.body = html.body.replace(b'</body>', ('<script src="' + src + '"></script></body>').encode(), 1)
        html.headers['content-length'] = str(len(html.body))
        return html

    route.app = request_response(with_helper)
