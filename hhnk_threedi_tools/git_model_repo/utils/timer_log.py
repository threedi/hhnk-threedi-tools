import logging
import time

log = logging.getLogger("performance")


class SubTimer:
    """
    Timer for task
    usage:
    with SubTimer("task_name"):
        do_something()

    or:
    timer = SubTimer("task_name")
    do_something()
    timer.finish()

    """

    def __init__(self, name):
        """Start a timer
        use .finish() to stop the timer.
        """
        self.name = name
        self.start = time.time()
        self.process_duration = time.process_time()

        if name:
            log.debug(f"{name} started")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.finish()

    def finish(self):
        log.info(
            f"{self.name} finished - duration: {time.time() - self.start:.2f}, process_duration: {time.process_time() - self.process_duration:.2f}"
        )
