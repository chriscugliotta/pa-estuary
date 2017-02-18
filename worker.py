# External imports
import logging
import logging.config
import multiprocessing as mp
import time
import random
import sys

# Internal imports
from config import S3Config
from log import LogHelper
from s3 import S3Helper
from task import StatusMessage, TestTask, ImportTask

# Logger
log = logging.getLogger()



class Worker(mp.Process):
    """
    A worker process.

    There can be multiple workers, each working in parallel.  Each worker
    monitors the task queue.  When a task arrives, a worker claims it, and
    begins working on it.  When finished, the worker returns to monitoring the
    queue, and waits for another task to arrive.

    Attributes:
        log_queue:  We send logging messages here.
        task_queue:  We receive open, unclaimed tasks from here.
        status_queue:  We can send important status updates to main via this.
        stop:  Equals true if worker process should stop looping.
    """

    def __init__(self, log_queue, task_queue, status_queue):
        super(Worker, self).__init__()
        self.log_queue = log_queue
        self.task_queue = task_queue
        self.status_queue = status_queue
        self.stop = False

    def run(self):
        """
        The entry point for a worker process.  Called by Process.start().
        """

        # Configure log
        log_helper = LogHelper(self.log_queue)
        log_helper.configure()
        log.info('Begin')

        # Initialize helpers
        s3 = S3Helper()

        # Loop
        while not self.stop:

            # Wait for next message
            task_message = self.task_queue.get()
            # Convert message to task
            task = task_message.to_task(self, s3)
            log.info('Claiming task = {0}'.format(task_message))

            # Do task
            try:
                task.do()
                task.status_message.value = 'success'
                self.status_queue.put(task.status_message)
            except:
                log.error('Error: {0}'.format(sys.exc_info()))
                task.status_message.value = 'failure'
                self.status_queue.put(task.status_message)
            finally:
                task.cleanup()

        # Log
        log.info('End')