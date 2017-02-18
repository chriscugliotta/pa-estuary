# External imports
import logging
import boto3

# Internal imports
from config import S3Config

# Logging
log = logging.getLogger()

# Silence third party loggers
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)



class S3Helper:
    """
    A helper class for interacting with S3.

    Note:
        The underlying API is documented here:
        - http://boto3.readthedocs.io/en/latest/reference/services/s3.html#client

    Attributes:
        client:  A boto3 S3 Client object.  Used for S3 interactions.
    """

    def __init__(self):
        """
        Initializes the helper object.

        Note:
            To be safe, we do not reference the AWS access/secret keys anywhere
            in the code.  Instead, we store them in environment variables
            AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, respectively.  If
            these environment variables aren't defined, this helper class will
            not work properly.
        """

        # Instantiate client
        self.client = boto3.client('s3')

        """
        If you need to provide keys directly, you can do this...
        self.client = boto3.client(
            's3',
            aws_access_key_id=S3Config.access_key,
            aws_secret_access_key=S3Config.secret_key
        )
        """

    def list_files(self, bucket, prefix):
        """
        Queries a bucket, and returns a list of all keys beginning with a
        certain prefix.  In other words, we're getting a list of all files
        within a certain folder.

        Note:
            The API only lets you query (at most) one thousand files at a time.
            This method will loop, and collect one chunk at a time, until the
            full set is obtained.

        Args:
            bucket:  We will only return files within this bucket.
            prefix:  We will only return files within this folder.

        Returns:
            A list of dictionaries.
            Each dictionary represents a single file's metadata.
            The dictionary structure comes directly from boto3 response object.
        """

        # Log
        log.debug('Begin list_files with prefix = {0}'.format(prefix))

        # Initialize loop parameters
        result = []
        done = False
        marker = ''
        max_keys = 1000
        total_keys = 0

        # Begin loop
        while not done:

            # Get chunk of objects
            response = self.client.list_objects(
                Bucket=bucket,
                Prefix=prefix,
                Marker=marker,
                MaxKeys=max_keys
            )

            # If prefix doesn't exist, stop
            if 'Contents' not in response:
                break

            # Add objects to result
            for object in response['Contents']:
                # Debug
                log.debug(object)
                # Add to list
                result.append(object)
                # Increment
                total_keys += 1

            # Check if done
            if response['IsTruncated']:
                marker = response['Contents'][-1]['Key']
            else:
                done = True

        # Log and return
        log.debug('Done list_files with prefix = {0}, total_keys = {1}'.format(prefix, total_keys))
        return result

    def upload_file(self, local, bucket, remote):
        """
        Uploads a local file to S3.

        Args:
            local:  The local file to upload, e.g. 'C:/uploadMe.txt'.
            bucket:  The remote S3 bucket.
            remote:  The remote destination key, e.g. 'pag_estuary/uploaded.txt'.

        Returns:
            The boto3 response object.
        """

        # Log
        log.debug('Begin upload_file with local = {0}, remote = {1}'.format(local, remote))

        # Upload
        response = self.client.upload_file(local, bucket, remote)

        # Log and return
        log.info('Done upload_file with local = {0}, remote = {1}'.format(local, remote))
        return response

    def download_file(self, bucket, remote, local):
        """
        Downloads a file from S3.

        Args:
            bucket:  The remote S3 bucket.
            remote:  The remote file to download, e.g. 'pag_estuary/downloadMe.txt'.
            local:  Where to save the downloaded file, e.g. 'C:/downloaded.txt'.

        Returns:
            The boto3 response object.
        """

        # Log
        log.debug('Begin download_file with remote = {0}, local = {1}'.format(remote, local))

        # Download
        response = self.client.download_file(S3Config.bucket, remote, local)

        # Log and return
        log.info('Done download_file with remote = {0}, local = {1}'.format(remote, local))
        return response

    def copy_file(self, bucket, source, destination):
        """
        Copies an S3 object.

        Args:
            bucket:  The S3 bucket.
            source:  The source path, e.g. 'pag_estuary/copyMe.txt'.
            destination:  The destination path, e.g. 'pag_estuary/copied.txt'.

        Returns:
            The boto3 response object.
        """

        # Copy
        response = self.client.copy_object(
            Bucket=bucket,
            CopySource='{0}/{1}'.format(bucket, source),
            Key=destination
        )

        # Log and return
        log.info('Done copy_file with source = {0}, destination = {1}'.format(source, destination))
        return response

    def delete_file(self, bucket, key):
        """
        Deletes an S3 object.

        Args:
            bucket:  The S3 bucket.
            key:  The key to delete, e.g. 'pag_estuary/deleteMe.txt'.

        Returns:
            The boto3 response object.
        """

        # Delete
        response = self.client.delete_object(
            Bucket=bucket,
            Key=key
        )

        # Log and return
        log.info('Done delete_file with key = {0}'.format(key))
        return response

    def move_file(self, bucket, source, destination):
        """
        Moves or renames an S3 object.

        Args:
            bucket:  The S3 bucket.
            source:  The source key, e.g. 'pag_estuary/pending/beforeMove.txt'.
            destination:  The destination key, e.g. 'pag_estuary/rejected/afterMove.txt'.
        """

        # Copy, then delete
        self.copy_file(bucket, source, destination)
        self.delete_file(bucket, source)