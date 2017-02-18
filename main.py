# External imports
import datetime
import logging
import logging.config
import logging.handlers
import multiprocessing as mp
import random
import sys
import time

# Internal imports
from config import AppConfig, S3Config
from log import LogHelper
from s3 import S3Helper
from task import TaskMessage
from worker import Worker

# Logger
log = logging.getLogger()



class Main:
    """
    The main process.

    The main process is responsible for spawning worker processes, checking for
    new tasks by monitoring external systems (i.e. S3), and assigning tasks to
    its pool of workers.

    Attributes:
        log_queue:  Workers send log messages here.  See LogHelper.
        task_queue:  Workers check for new tasks here.
        status_queue:  Workers can send updates back to main via this queue.
        log_helper:  Helps configure multiprocess logging.
        workers:  A list of all worker processes.
        looped_functions:  An ordered list of functions.  All functions are
            called, one at a time, each iteration of the main process loop.
        s3:  Helps interact with S3.
        max_s3_time:  The maximum 'LastModified' value of all imported files.
            On subsequent iterations, we should only import files newer than
            this.
        stop:  Equals true if main process should stop looping.

    """

    def __init__(self):
        """
        The main process initialization.  Configures logging, spawns workers.
        """

        # Initialize queues
        self.log_queue = mp.Queue()
        self.task_queue = mp.Queue()
        self.status_queue = mp.Queue()

        # Configure log
        self.log_helper = LogHelper(self.log_queue)
        self.log_helper.configure()
        log.info('Begin')

        # Initialize workers
        self.workers = []
        for i in range(AppConfig.worker_count):
            worker = Worker(self.log_queue, self.task_queue, self.status_queue)
            self.workers.append(worker)
            worker.start()

        # Initialize looped functions
        self.looped_functions = []
        self.looped_functions.append(self.check_s3)
        self.looped_functions.append(self.check_status)

        # Initialize S3 helper
        self.s3 = S3Helper()

        # Initialize maximum S3 time as 'negative infinity'
        self.max_s3_time = datetime.datetime(1, 1, 1, 0, 0, 0, 0, datetime.timezone.utc)

        # Don't stop until told
        self.stop = False

    def run(self):
        """
        The main process loop.  Checks for new tasks, and adds tasks to queue.

        Note:
            This behavior can be customized by adding or removing elements from
            the looped_functions list.  This is useful for testing or extending
            the code.
        """

        # Main process loop
        while not self.stop:

            # Call all functions
            for f in self.looped_functions:
                try:
                    f()
                except:
                    log.error('Error: {0}'.format(sys.exc_info()))
                finally:
                    pass

            # Sleep
            time.sleep(AppConfig.sleep_time)
            # TODO:  Instead of sleeping, we should continuously monitor status queue during downtime...

        # Clean up and exit
        for worker in self.workers:
            worker.join()
        self.log_helper.listener.stop()
        log.info('End')

    def check_status(self):
        """
        Checks the status queue for messages from workers.
        Currently, this is just a placeholder.
        We don't actually do anything with this information yet.
        """
        while not self.status_queue.empty():
            message = self.status_queue.get(True, 5)
            log.info('Received status message: {0}'.format(message))

            # Example shut down...
            # task_message = TaskMessage('KillSelfTask')
            # self.task_queue.put(task_message)
            # log.info('Added message to queue: {0}'.format(task_message))
            # self.stop = True

    def check_s3(self):
        """
        Scans S3 for new files, and creates an ImportTask for each.

        Note:
            How do we prevent double-creating a task?  Currently, we're only
            looking at LastModified attribute.  If we create a task, and then
            someone quickly modifies/overwrites a file (before it leaves
            'pending' folder), our algorithm might create a 2nd task for that
            same file.  This could lead to a double-import...

            Can this be protected at database level?  If we have a database
            table that archives historical (and in-progress) imports, then the
            duplicate task will realize it's a duplicate, and then kill itself.
            So, we might be OK...
        """

        # Check S3 'pending' folder
        log.debug('Begin check_s3 with max_s3_time = {0}'.format(self.max_s3_time))
        results = self.s3.list_files(S3Config.bucket, S3Config.root + '/pending/')

        # This will eventually become the NEW value of max_s3_time...
        new_s3_time = self.max_s3_time

        # Loop
        for result in results:

            # If file size is zero, skip
            if result['Size'] == 0:
                continue

            # If timestamp is 'old', we've already created a task, so skip
            if result['LastModified'] <= self.max_s3_time:
                continue

            # Update max
            if result['LastModified'] > new_s3_time:
                new_s3_time = result['LastModified']

            # Add new task to queue
            message = TaskMessage('ImportTask', {'file_path': result['Key']})
            self.task_queue.put(message)
            log.info('Added message to queue: {0}'.format(message))

        # On subsequent iterations, we should only import files newer than this
        self.max_s3_time = new_s3_time
        log.info('Done check_s3 with max_s3_time = {0}'.format(self.max_s3_time))

        """
        To check all Estuary files on S3 periodically...
        results = self.s3.list_files(S3Config.bucket, S3Config.root)
        for result in results:
            if result['Size'] > 0:
                log.info(result)
        """

    def create_test_task(self):
        """
        Creates a TestTask.
        """
        message = TaskMessage('TestTask', {'wait_time': 2})
        self.task_queue.put(message)
        log.info('Added message to queue: {0}'.format(message))



# Entry point
if __name__ == '__main__':
    m = Main()
    m.run()
