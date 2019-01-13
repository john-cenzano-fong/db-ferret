from datetime import datetime, timedelta


def elapsed_time(seconds):
    sec = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + sec
    return "%d hr %d min %d sec" % (d.hour, d.minute, d.second)


def incremental_marker(count, increment=5):
    if (count + 1) % increment == 0:
        return "*"
    else:
        return " "
