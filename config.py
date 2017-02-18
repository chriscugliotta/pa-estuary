class AppConfig:
    """
    Miscellaneous application parameters.
    """

    # The number of parallel worker processes
    worker_count = 1

    # The rate (in seconds) at which the main process scans S3 for new files
    sleep_time = 15

    # Local file system root folder
    root = 'C:/Altegra/Code/pa-estuary'



class S3Config:
    """
    Miscellaneous S3 parameters.
    """

    # The bucket containing all Estuary files
    bucket = 'aws-dwred-01.altegrahealth.com'

    # The root folder containing all Estuary files
    root = 'pag_estuary'

    # The access keys
    access_key = 'See environment variable: AWS_ACCESS_KEY_ID'
    secret_key = 'See environment variable: AWS_SECRET_ACCESS_KEY'