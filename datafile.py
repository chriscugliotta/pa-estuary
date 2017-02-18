class DataFile:
    """
    Abtract base class for data files.

    Each subclass should represent a standardized data file, e.g. MMR, MOR,
    etc.  All subclasses should override the 'preprocess' and 'postprocess'
    methods.
    """

    def preprocess(self):
        raise NotImplementedError('DataFile subclass must override preprocess()')

    def postprocess(self):
        raise NotImplementedError('DataFile subclass must override postprocess()')



class Test1File(DataFile):
    pass



class Test2File(DataFile):
    pass



class Test3File(DataFile):
    pass



class MMRFile(DataFile):
    pass



class MORFile(DataFile):
    pass