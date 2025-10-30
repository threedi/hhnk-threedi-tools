import logging
import time

logger = logging.getLogger("performance")


class SubTimer:
    """Timer for task. Can be used with 'With' statement to automatically log the duration of a task.

    Parameters
    ----------
    name : str
        Name of the task to be timed.

    Attributes
    ----------
    name : str
        Name of the task.
    start : float
        Start time in seconds since the epoch.
    process_duration : float
        Start process time in seconds.

    Methods
    -------
    finish()
        Log the duration and process duration of the task.

    Example
    -------
    >>> with SubTimer("My Task"):
    >>>     # Your code here
    >>>     time.sleep(2)  # Simulating a task
    >>> # Output: My Task finished - duration: 2.00, process_duration: 2.00

    """

    def __init__(self, name: str):
        """Start a timer.

        Parameters
        ----------
        name : str
            Name of the task to be timed.
        """
        self.name: str = name
        self.start: float = time.time()
        self.process_duration: float = time.process_time()

        if name:
            logger.debug(f"{name} started")

    def __enter__(self) -> "SubTimer":
        """Enter the runtime context related to this object.

        Returns
        -------
        SubTimer
            The timer object itself.
        """
        return self

    def __exit__(self, *args) -> None:
        """Exit the runtime context and finish the timer.

        Parameters
        ----------
        *args
            Exception information (if any).

        Returns
        -------
        None
        """
        self.finish()

    def finish(self) -> None:
        """Log the duration and process duration of the task.

        Returns
        -------
        None
        """
        logger.info(
            f"{self.name} finished - duration: {time.time() - self.start:.2f}, process_duration: {time.process_time() - self.process_duration:.2f}"
        )
