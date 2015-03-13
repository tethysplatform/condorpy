# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have be distributed with this file.

from setuptools import setup, find_packages
import os

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name = "condorpy",
    version = "0.0.0",
    packages = find_packages(),
    #scripts = ['say_hello.py'],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    install_requires = ['tethyscluster'],
    

#    package_data = {
#        # If any package contains *.txt or *.rst files, include them:
#        '': ['*.txt', '*.rst'],
#        # And include any *.msg files found in the 'hello' package, too:
#        'hello': ['*.msg'],
#    },

    
    # metadata for upload to PyPI
    author = "Scott Christensen",
    author_email = "sdc50@byu.net",
    description = "A Python wrapper for HTCondor's cli",
    long_description=README,
    license = "BSD 2-Clause License",
    keywords = "htcondor distributed-computing job-scheduling",
    url = "https://bitbucket.org/sdc50/condorpy/wiki/Home",   # project home page, if any

    # could also include download_url, classifiers, etc.
)