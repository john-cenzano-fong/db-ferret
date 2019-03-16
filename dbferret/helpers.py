# -*- coding: utf-8 -*-
import __future__
from datetime import datetime, timedelta
import os


def elapsed_time(seconds):
    """
    When provided with a number of seconds, output a human readable 
    string representing elapsed time
    :param seconds: integer containing the number of seconds
    :return: string with elapsed time
    """
    sec = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + sec
    return "%d hr %d min %d sec" % (d.hour, d.minute, d.second)


def incremental_marker(count, interval=5, range_start=0):
    """
    Return back a visual marker if the counter lands on a multiple of
    a specified interval. This is meant to append to results output
    when processing an iterator.
    
    :param count: integer containing counter number
    :param interval: integer containing the interval to which counter 
                     should be compared
    :param range_start: start position of range that counter is incrementing
                        through. For example 0 means it is a 0 index based
                        range, so a counter of 2 would represent the 3rd
                        item in a list.
    :return: string with marker value
    """
    position = count + 1 - range_start
    return "*" if position % interval == 0 else " "


def create_directory(directory="data", root="."):
    """
    Safely create a directory if it doesn't exist
    :param dir:
    "return: string containing the path
    """
    target = os.path.join(root, directory)
    if not os.path.exists(target):
       os.makedirs(target)
    return target
