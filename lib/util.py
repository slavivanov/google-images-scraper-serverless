import sys
import os

def process_exception(exception):
    """ Currently stop exception propagation and only print 
        information about exception, so that we don't 
        have Lambda retries.
        Eventually, this can be tied with a notification system.
    Args:
        exception: The raised exception.
    """
    error_traceback = sys.exc_info()[-1]
    error_filename = os.path.split(
        error_traceback.tb_frame.f_code.co_filename)[1]
    print('Error "{}" on line {} in file {}'.format(
        str(exception), 
        error_traceback.tb_lineno, 
        error_filename))
