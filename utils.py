from datetime import datetime, timedelta


def elapsed_time(seconds):
    """
    When provided with a number of seconds, output a human readable 
    string representing elapsed time

    :param seconds: integer containing the number of seconds
    :return: string
    """
    sec = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + sec
    return "%d hr %d min %d sec" % (d.hour, d.minute, d.second)


def incremental_marker(count, interval=5):
    """
    Return back a visual marker if the counter lands on a multiple of
    a specified interval. This is meant to append to results output
    when processing an iterator.
    
    :param count: integer containing counter number
    :param interval: integer containing the interval to which counter 
                     should be compared
    :return: string
    """
    if (count + 1) % interval == 0:
        return "*"
    else:
        return " "
