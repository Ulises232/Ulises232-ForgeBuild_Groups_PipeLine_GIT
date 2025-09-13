
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

_executor = ThreadPoolExecutor(max_workers=4)

def run_bg(fn: Callable, *args, on_ok: Callable | None = None, on_err: Callable | None = None, **kwargs):
    f = _executor.submit(fn, *args, **kwargs)
    def _done(fut):
        try:
            res = fut.result()
            if on_ok: on_ok(res)
        except Exception as e:
            if on_err: on_err(e)
    f.add_done_callback(_done)
    return f
