# External imports
import logging
import os
import shutil
import time

# Internal imports
from config import AppConfig, S3Config
from s3 import S3Helper

# Logger
log = logging.getLogger()



class TaskMessage:
    """
    A message representing a task.

    In a perfect world, we would place Task objects in the task queue.
    Unfortunately, mp.Queue doesn't like complicated objects.  Tasks can be
    complex.  For instance, if a Task has an instance variable that isn't
    serializable, mp.Queue will throw an error.  Thus, instead of placing Tasks
    directly in the queue, we will instead place lightweight TaskMessage
    objects.  When a worker receives the message, it will convert it into the
    appropriate Task object, and then perform the workload.

    Attributes:
        id:  A unique identifier.
        type:  The type of task this message represents.  It should match a
            class name exactly, e.g. TestTask, ImportTask, etc.
        args:  A dictionary of arguments for the task initializer.
    """

    # A unique ID generator
    next_id = 0

    def __init__(self, type, args={}):
        TaskMessage.next_id += 1
        self.id = TaskMessage.next_id
        self.type = type
        self.args = args

    def __str__(self):
        return '{{id: {0}, type: {1}, args: {2}}}'.format(self.id, self.type, self.args)

    def to_task(self, worker, s3):
        """
        Converts the TaskMessage to a Task.

        Note:
            The code below is a condensed version of:
            if self.type == 'TestTask':   return TestTask(...)
            if self.type == 'ImportTask': return ImportTask(...)
            etc.
        """
        return globals()[self.type](worker, self, s3)



class StatusMessage:
    """
    A message representing the status of a task.

    Attributes:
        value:  Either 'in_progress', 'success' or 'failure'
        task_message:  The originating task message.
    """

    def __init__(self, value, task_message):
        self.value = value
        self.task_message = task_message

    def __str__(self):
        return '{{value: {0}, task_message: {1}}}'.format(self.value, self.task_message)



class Task():
    """
    Abstract base class for tasks.

    A task should represent a unit of work, e.g. importing a file into a db.
    All subclasses should override 'do' and 'cleanup' methods.
    All subclasses should call the superclass initializer.

    Attributes:
        worker:  The worker assigned to this task.
        task_message:  The originating task message.
        status_message:  Either 'in_progress', 'success' or 'failure'.
        s3:  Helps interact with S3.
    """

    def __init__(self, worker, task_message, s3):
        self.worker = worker
        self.task_message = task_message
        self.status_message = StatusMessage('in_progress', task_message)
        self.s3 = s3

    def do(self):
        raise NotImplementedError('Task subclass must override do()')

    def cleanup(self):
        raise NotImplementedError('Task subclass must override cleanup()')



class TestTask(Task):
    """
    A test task.  Simply waits X seconds.

    Attributes:
        wait_time:  The amount of time to wait until completion.
    """

    def __init__(self, worker, task_message, s3):
        super(TestTask, self).__init__(worker, task_message, s3)
        self.wait_time = task_message.args['wait_time']

    def do(self):
        log.info('Working.')
        time.sleep(self.wait_time / 3)
        log.info('Working..')
        time.sleep(self.wait_time / 3)
        log.info('Working...')
        time.sleep(self.wait_time / 3)
        log.info('Done!')

    def cleanup(self):
        pass



class KillSelfTask(Task):
    """
    Orders a worker process to kill itself.
    """

    def do(self):
        self.worker.stop = True
        log.warning('Killing self...')

    def cleanup(self):
        pass



class ImportTask(Task):
    """
    A file has been uploaded to S3, and should be imported into the database.

    Attributes:
        file_path:  Full path (i.e. S3 object key) of the file to be imported.
        client:  The client this file belongs to.
        file_type:  The file's data type, i.e. MMR, MOR, etc.
        version:  The file's version, e.g. Jan MMR vs. Feb MMR.
    """

    def __init__(self, worker, task_message, s3):
        super(ImportTask, self).__init__(worker, task_message, s3)
        self.file_path = task_message.args['file_path']

    def do(self):
        """
        A file has been uploaded to S3.  Our goal is to import it into the
        database.  We will accomplish this via the following steps:

            1.  Parse
                Parse the file name, and deduce important properties of the
                file, e.g. client, file type, version, etc.

            2.  Download
                Download file from S3 onto local machine.

            3.  Pre-process
                Prepare the file for database import.  For instance, we may
                need to unzip, convert to CSV, add/remove columns, etc.

            4.  Upload
                Next, we re-upload the processed file to S3.

            5.  Import
                Next, we import the processed file into the database.

            6.  Post-process
                Lastly, we may optionally perform some post-processing on the
                data.  For instance, perhaps we want to store a transformed
                duplication of the data in a separate reporting table.

        """
        self._parse()
        self._download()
        self._preprocess()
        self._upload()
        self._import()
        self._postprocess()

    def _parse(self):
        """
        Parses the file name.  Our goal is to determine three things:

            1.  Client:  By design, this can always be determined by the parent
                folder.

            2.  File type:  The Estuary will support a pre-determined list of
                standardized data files, e.g. MMR, MOR, etc.

            3.  Version:  Some files will have multiple versions over time,
                e.g. Jan 2017 MMR vs. Feb 2017 MMR.

        For now, we simplistically assume all files will obey the following
        pattern:  <filetype>_<yyyymmdd>.  If any file disobeys this pattern, it
        will be rejected.
        """

        # TODO:  Disclaimer... this code is horrible.  I will eventually rewrite it.

        # Log
        log.debug('Begin _parse with file_path = {0}'.format(self.file_path))

        # Remove root folder prefix, i.e. root/pending/clientName/fileName to pending/clientName/fileName
        s = self.file_path[len(S3Config.root) + 1:]
        # Decompose string
        elements = s.split('/')
        # There should be exactly 3 elements, i.e. pending/clientName/fileName
        assert len(elements) == 3, 'Invalid file_path: {0}'.format(self.file_path)
        # Client name is second element
        self.client = elements[1]
        # File name is third element
        self.file_name = elements[2]

        # Next, we decompose the file name
        elements = self.file_name.split('_')
        # There should be exactly 2 elements, i.e. <datatype>_<yyyymmdd>.ext
        assert len(elements) == 2, 'Invalid file_name: {0}'.format(self.file_name)
        # File type is first element
        self.file_type = elements[0]
        # File type should belong to predefined list
        assert self.file_type in ['test1', 'test2', 'test3', 'mmr', 'mor'], 'Invalid file_name: {0}'.format(self.file_name)
        # Version is second element
        self.version = elements[1]
        # TODO:  Validate version as YYYYMMDD...

        # Get paths
        self.download_folder = '{0}/data/download/{1}'.format(AppConfig.root, self.client)
        self.download_path = '{0}/data/download/{1}/{2}'.format(AppConfig.root, self.client, self.file_name)
        self.processed_folder = '{0}/data/processed/{1}'.format(AppConfig.root, self.client)
        self.processed_path = '{0}/data/processed/{1}/{2}'.format(AppConfig.root, self.client, self.file_name)
        self.upload_path = '{0}/processed/{1}/{2}'.format(S3Config.root, self.client, self.file_name)
        self.accepted_path = '{0}/accepted/{1}/{2}'.format(S3Config.root, self.client, self.file_name)
        self.rejected_path = '{0}/rejected/{1}/{2}'.format(S3Config.root, self.client, self.file_name)

        # Log
        log.info('Done _parse with file_path = {0}, client = {1}, file_type = {2}, version = {3}'.format(self.file_path, self.client, self.file_type, self.version))

    def _download(self):
        """
        First, locally, we create a client-specific 'download' folder.
        Then, we download the file from S3.
        """
        os.makedirs(self.download_folder, exist_ok=True)
        self.s3.download_file(S3Config.bucket, self.file_path, self.download_path)

    def _preprocess(self):
        """
        First, locally, we create a client-specific 'processed' folder.
        Then, we process the file, i.e. prepare it for database import.

        Note:
            This is just a placeholder.
            We don't actually process anything yet.
        """
        os.makedirs(self.processed_folder, exist_ok=True)
        shutil.copyfile(self.download_path, self.processed_path)

    def _upload(self):
        """
        Uploads the processed file to S3.
        """
        self.s3.upload_file(self.processed_path, S3Config.bucket, self.upload_path)

    def _import(self):
        """
        Imports the processed file into the database.

        Note:
            This is just a placeholder.
            We don't actually import anything yet.
        """
        pass

    def _postprocess(self):
        """
        Performs some post-processing on the data.

        Note:
            This is just a placeholder.
            We don't actually post-process anything yet.
        """
        pass

    def cleanup(self):
        """
        Called by worker, regardless of success or failure.
        """

        # Log
        log.debug('Begin cleanup with status_message = {0}'.format(self.status_message))

        # If task was successfully completed, move original file 'accepted' folder
        # Otherwise, move it to 'rejected' folder
        if self.status_message.value == 'success':
            self.s3.move_file(S3Config.bucket, self.file_path, self.accepted_path)
        else:
            self.s3.move_file(S3Config.bucket, self.file_path, self.rejected_path)

        # Clean up local 'download' folder
        try:
            os.remove(self.download_path)
            os.rmdir(self.download_folder)
        except:
            pass

        # Clean up local 'processed' folder
        try:
            os.remove(self.processed_path)
            os.rmdir(self.processed_folder)
        except:
            pass

        # Clean up S3 'processed' object
        self.s3.delete_file(S3Config.bucket, self.upload_path)

        # Log
        log.info('Done cleanup with status_message = {0}'.format(self.status_message))

