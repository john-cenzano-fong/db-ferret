# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='db-ferret',
    version='0.1.0',
    description='A tool for ferreting out metadata about databases',
    long_description=readme,
    author='John Cenzano-Fong',
    url='https://github.com/john-cenzano-fong/db-ferret',
    license=license,
    packages=find_packages(exclude=('tests'))
)
