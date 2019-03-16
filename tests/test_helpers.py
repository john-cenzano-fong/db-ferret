# -*- coding: utf-8 -*-
import os

from context import helpers


def test_elapsed_time():
    assert helpers.elapsed_time(0) == "0 hr 0 min 0 sec"
    assert helpers.elapsed_time(37) == "0 hr 0 min 37 sec"
    assert helpers.elapsed_time(61) == "0 hr 1 min 1 sec"
    assert helpers.elapsed_time((5 * 60) + 1) == "0 hr 5 min 1 sec"
    assert helpers.elapsed_time((61 * 60) + 1) == "1 hr 1 min 1 sec"
    assert helpers.elapsed_time((60 * 60 * 12) + 18) == "12 hr 0 min 18 sec"


def test_incremental_marker():
    assert helpers.incremental_marker(2) == " "
    assert helpers.incremental_marker(4) == "*"
    assert helpers.incremental_marker(9) == "*"
    assert helpers.incremental_marker(9, 10) == "*"
    assert helpers.incremental_marker(9, 10, 0) == "*"
    assert helpers.incremental_marker(9, 10, 1) == " "
    assert helpers.incremental_marker(10, 10, 1) == "*"
    assert helpers.incremental_marker(110, 10, 101) == "*"


def test_create_directory(tmpdir):
    # Create temporary location
    test_dir = tmpdir.mkdir("directory")

    default_dir = helpers.create_directory(root=str(test_dir))
    explicit_dir = helpers.create_directory(
        root=str(test_dir), directory="explicit")
    assert os.path.exists(default_dir)
    assert str(test_dir.join("data")) == default_dir
    assert os.path.exists(explicit_dir)
    assert str(test_dir.join("explicit")) == explicit_dir
