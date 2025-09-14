# buildtool/tests/errguard_smoke.py
from __future__ import annotations
import sys, threading, asyncio, time
from buildtool.core.errguard import install_error_guard, log, on_about_to_quit_flush

def uncaught_exception():
    # Esto debe ser capturado por sys.excepthook de errguard
    raise RuntimeError("SMOKE: uncaught_exception()")

def thread_crash():
    def _t():
        raise ValueError("SMOKE: thread_crash() in worker thread")
    th = threading.Thread(target=_t, name="smoke-thread")
    th.start()
    th.join()

def asyncio_crash():
    async def boom():
        await asyncio.sleep(0.01)
        raise LookupError("SMOKE: asyncio_crash() in task")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(lambda l, c: log(f"Loop handler caught: {c}", level=30))  # WARNING
    loop.create_task(boom())
    try:
        loop.run_until_complete(asyncio.sleep(0.05))
    finally:
        loop.close()

def main():
    install_error_guard(app_name="buildtool", verbose=True)
    log("== SMOKE CLI start ==")

    # 1) Excepción en hilo
    thread_crash()
    # 2) Excepción en asyncio
    asyncio_crash()
    # 3) Excepción no atrapada (al final, para no cortar antes)
    try:
        uncaught_exception()
    finally:
        on_about_to_quit_flush()

if __name__ == "__main__":
    main()
