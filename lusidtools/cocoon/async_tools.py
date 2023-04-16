import asyncio
import functools
from threading import Thread, enumerate
import concurrent.futures


def start_event_loop_new_thread() -> asyncio.AbstractEventLoop:
    """
    Creates and starts a new event loop in a new thread

    Returns
    -------
    loop : asyncio.AbstractEventLoop
        The newly created loop

    """

    loop = asyncio.new_event_loop()
    t = Thread(target=start_background_loop, args=(loop,), daemon=True)
    t.start()
    return loop


def stop_event_loop_new_thread(loop: asyncio.AbstractEventLoop) -> None:
    """
    Takes an event loop, stops it and once stopped closes it

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The loop to stop and close

    """

    thread_id = loop._thread_id
    loop.call_soon_threadsafe(loop.stop)
    threads = enumerate()
    match = [thread for thread in threads if thread.ident == thread_id]
    if len(match) == 1:
        match[0].join(timeout=1)


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    """
    Sets and starts the event loop

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop to use

    """

    asyncio.set_event_loop(loop)
    loop.run_forever()


def run_in_executor(f):
    """
    Passes a synchronous & blocking function off to another thread so that it can be awaited

    Parameters
    ----------
    f
        The function to transform into an awaitable

    Returns
    -------
    inner : callable
        An awaitable version of the function with the execution delegated to another thread
    """

    @functools.wraps(f)
    def inner(*args, **kwargs):
        loop = asyncio.get_running_loop()

        return loop.run_in_executor(
            # If the function to be wrapped has been provided with a thread pool use that, otherwise create one
            kwargs.get("thread_pool", ThreadPool(5).thread_pool),
            lambda: f(*args, **kwargs),
        )

    return inner


class ThreadPool:
    """
    Creates a class which has a thread pool.
    """

    def __init__(self, max_workers):
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers
        )
