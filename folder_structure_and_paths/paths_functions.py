import os

def get_top_level_directories(folder, condition_test=None):
    """
    Resturns a list of all top level directories, can be filtered with a function (condition_test)
    that returns a bool and takes one argument (directory)
    """
    return [item for item in (os.path.join(folder, d1) for d1 in os.listdir(folder))
            if os.path.isdir(item) and (condition_test(item) if condition_test is not None else True)]
