import logging
import logging.config
import logging.handlers
import multiprocessing as mp

class LogHelper():
    """
    A helper class for setting up logging across multiple parallel processes.

    We want all parallel processes to write to a common log file.  However, an
    error will occur if two processes write to the same file simultaneously.
    Python's 'logging' module offers some features to circumvent this problem.
    It's tricky getting 'logging' and 'multiprocessing' modules to play nicely
    together.  This class aims to encapsulate the multiprocess logging setup.

    This implementation is inspired by:
        - https://gist.github.com/vsajip/2331314
        - https://docs.python.org/3.5/howto/logging-cookbook.html
   """

    def __init__(self, q):
        """
        Initializes a LogHelper object.

        Note:
            The logging queue should be instantiated exactly once, and only in
            the main process.  Why?  Because all parallel processes must use
            the same queue instance.  Thus, we should create a logging queue in
            the main process, then pass it as an argument to each worker
            process.  This way, all processes share the exact same queue.
        """

        # Store the logging queue.  All worker processes will pass log messages
        # to this queue.  The main process will listen to this queue, and write
        # any inbound messages to a text file.
        self.q = q

        # The worker processes will use this logging configuration.  Basically,
        # instead of writing log messages directly to text, we simply pass them
        # to the queue.
        self.config_worker = {
            'version': 1,
            'disable_existing_loggers': True,
            'handlers': {
                'queue': {
                    'class': 'logging.handlers.QueueHandler',
                    'queue': self.q,
                },
            },
            'root': {
                'level': 'INFO',
                'handlers': ['queue']
            },
        }

        # The main process will use this logging configuration.  Basically,
        # the QueueListener will monitor the queue, and handle any inbound
        # messages via this config.
        self.config_main = {
            'version': 1,
            'disable_existing_loggers': True,
            'formatters': {
                'simple': {
                    'class': 'logging.Formatter',
                    'format': '%(asctime)s %(levelname)-8s %(processName)-11s %(message)s'
                },
                'detailed': {
                    'class': 'logging.Formatter',
                    'format': '%(asctime)s\t%(levelname)s\t%(processName)s\t%(filename)s\t%(funcName)s\t%(message)s'
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'formatter': 'simple',
                },
                'file': {
                    'class': 'logging.FileHandler',
                    'filename': 'log.log',
                    'mode': 'w',
                    'formatter': 'detailed',
                },
            },
            'root': {
                'level': 'INFO',
                'handlers': ['console', 'file']
            },
        }

    def configure(self):
        """
        Configures logger, depending on process.
        """
        if mp.current_process().name == 'MainProcess':
            self.configure_main()
        else:
            self.configure_worker()

    def configure_main(self):
        """
        Configures logger on main process.

        Note:
            This method should only be called from the main process!

        Returns:
            The result of logging.getLogger(), but with all configs applied.
        """

        # Apply config
        logging.config.dictConfig(self.config_main)
        # Get logger
        log = logging.getLogger()
        # Instantiate listener
        self.listener = logging.handlers.QueueListener(self.q, *log.handlers)
        # Start listener
        self.listener.start()
        # Return logger
        return log

    def configure_worker(self):
        """
        Configures logger on worker process.

        Note:
            This method should only be called from a worker process!

        Returns:
            The result of logging.getLogger(), but with all configs applied.
        """

        # Apply config
        logging.config.dictConfig(self.config_worker)
        # Return logger
        return logging.getLogger()