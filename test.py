# External imports
import datetime
import logging
import os
import unittest
import multiprocessing as mp

# Internal imports
from config import AppConfig, S3Config
from log import LogHelper
from main import Main
from s3 import S3Helper

# Logging
log = logging.getLogger()



class ImportTestCase(unittest.TestCase):
    """
    Tests ImportTask.
    """

    def setUp(self):
        """
        In preparation for these tests, we will:

            1.  Create some sample CSV files on our local machine.

            2.  Upload these CSV files to S3, so they are noticed by the main
                process' S3 monitor.  It should create an ImportTask for each.

            3.  Create an empty Redshift table.  Eventually, the CSV files
                should get imported here.
        """

        # Configure log
        log_helper = LogHelper(mp.Queue())
        log_helper.configure()
        log.debug('Begin setUp')

        # Create local test directories
        os.makedirs(AppConfig.root + '/data/test/test_client1', exist_ok=True)
        os.makedirs(AppConfig.root + '/data/test/test_client2', exist_ok=True)

        # Create first CSV file
        # This is a relatively clean file
        with open(AppConfig.root + '/data/test/test_client1/test1_20170101.csv', 'w') as file:
            file.write('MEMBER_ID,FIRST_NAME,LAST_NAME,DOB\n')
            file.write('000000001,Chris,Cugliotta,11/27/1985\n')
            file.write('000000002,Naya,Cugliotta,4/1/2014\n')
            file.write('000000003,Ellie,Cugliotta,5/1/2014\n')

        # Create second CSV file
        # This is intentionally messy...
        # Optionally enclosed by quotes, escaped double quote, etc.
        with open(AppConfig.root + '/data/test/test_client1/test2_20170101.csv', 'w') as file:
            file.write('diag,diag_type,diag_desc\n')
            file.write('1,ICD-9,Common cold\n')
            file.write('1,ICD-10,"Common cold, with complications"\n')
            file.write('2,ICD-9,"Common cold, with "" complications"\n')
            file.write('2,ICD-10,Strep!@ Throat\n')

        # Create a third CSV file
        # Again, this is intentionally messy...
        # Multiple date formats, null values, extra blank lines, etc.
        with open(AppConfig.root + '/data/test/test_client1/test3_20170101.csv', 'w') as file:
            file.write('claim_id|claim_type|from_date|to_date\n')
            file.write('1||01-JAN-17|31-JAN-17\n')
            file.write('1|A|01-FEB-17|28-FEB-17\n\n')
            file.write('1|B|01-MAR-17|\n\n')

        # Create a fourth CSV file
        # This is similar to the first, but for a different client
        with open(AppConfig.root + '/data/test/test_client2/test1_20170101.csv', 'w') as file:
            file.write('MEMBER_ID,FIRST_NAME,LAST_NAME,DOB\n')
            file.write('000000004,Homer,Simpson,1/1/1975\n')

        # Upload files to S3
        self.s3 = S3Helper()
        self.s3.upload_file(AppConfig.root + '/data/test/test_client1/test1_20170101.csv', S3Config.bucket, S3Config.root + '/pending/test_client1/test1_20170101.csv')
        self.s3.upload_file(AppConfig.root + '/data/test/test_client1/test2_20170101.csv', S3Config.bucket, S3Config.root + '/pending/test_client1/test2_20170101.csv')
        self.s3.upload_file(AppConfig.root + '/data/test/test_client1/test3_20170101.csv', S3Config.bucket, S3Config.root + '/pending/test_client1/test3_20170101.csv')
        self.s3.upload_file(AppConfig.root + '/data/test/test_client2/test1_20170101.csv', S3Config.bucket, S3Config.root + '/pending/test_client2/test1_20170101.csv')

        # TODO:  Create empty Redshift tables...

        # Log
        log.info('Done setUp')

    def tearDown(self):
        """
        Clean up local files.
        Clean up S3.
        Clean up Redshift.
        """

        # Log
        log.debug('Begin tearDown')

        # Clean up local files
        for folder in ['download', 'processed', 'test']:
            for client in ['test_client1', 'test_client2']:
                for file in ['test1', 'test2', 'test3']:
                    path = '{0}/data/{1}/{2}/{3}_20170101.csv'.format(AppConfig.root, folder, client, file)
                    try:
                        os.remove(path)
                    except:
                        pass

        # Clean up local folders
        for folder in ['download', 'processed', 'test']:
            for client in ['test_client1', 'test_client2']:
                path = '{0}/data/{1}/{2}'.format(AppConfig.root, folder, client)
                try:
                    os.rmdir(path)
                except:
                    pass
        self.s3 = S3Helper()

        # Clean up S3
        for folder in ['pending', 'processed', 'accepted', 'rejected']:
            for client in ['test_client1', 'test_client2']:
                for file in ['test1', 'test2', 'test3']:
                    path = '{0}/{1}/{2}/{3}_20170101.csv'.format(S3Config.root, folder, client, file)
                    self.s3.delete_file(S3Config.bucket, path)

        # Check S3
        self.s3.list_files(S3Config.bucket, S3Config.root)

        # TODO:  Delete Redshift tables

        # Log
        log.info('Done tearDown')

    def test(self):

        # Log
        log.debug('Begin test')

        # Run
        m = Main()
        m.run()

        # TODO:  Inject an artificial stopping condition into the main process, e.g. after 4 completed tasks.
        # TODO:  This can be done via Main.looped_functions attribute...
        # TODO:  This is needed because, currently, process lives forever and thus tearDown is never called.

        # Log
        log.info('Done test')



# Test entry point
if __name__ == '__main__':
    unittest.main(warnings='ignore')