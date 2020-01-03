import asyncio
import functools
from threading import Thread


def start_event_loop_new_thread() -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    t = Thread(target=start_background_loop, args=(loop,), daemon=True)
    t.start()
    return loop


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Sets and starts the event loop

    :param asyncio.AbstractEventLoop loop: The event loop to use

    :return: None
    """
    asyncio.set_event_loop(loop)
    loop.run_forever()


def run_in_executor(f):
    """
    Passes a synchronous & blocking function off to another thread so that it can be awaited

    :param f: The function to transform into an awaitable

    :return: An awaitable version of the function with the execution delegated to another thread
    """

    @functools.wraps(f)
    def inner(*args, **kwargs):
        loop = asyncio.get_running_loop()

        return loop.run_in_executor(None, lambda: f(*args, **kwargs))

    return inner
