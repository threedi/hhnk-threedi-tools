import logging
import os

log = logging.getLogger('')


def setup_logging(level=logging.INFO):
    """Setup logging to git_log.log file in the root of the repo.

    """
    # log to git_log.log
    log.setLevel(level)
    handler = logging.FileHandler(
        os.path.join(os.path.dirname(__file__), os.pardir, 'git_log.log')
    )
    log.addHandler(handler)
    # format including sourcefile and datetime
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s - %(filename)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

    handler.setFormatter(formatter)
