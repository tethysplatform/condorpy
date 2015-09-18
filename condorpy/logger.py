# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.

import logging

DEBUGGING = False
LOGGING_LEVEL = logging.WARN

class DebugFilter(object):

    def filter(self, record):
        return DEBUGGING and record.levelno == logging.DEBUG

# set up logging to file - this sets up the default logging for the root handler
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
#                     datefmt='%m-%d %H:%M',
#                     filename='example.log',
#                     filemode='w')

# add the handlers to logger
log = logging.getLogger('condorpy')
log.setLevel(logging.DEBUG)
log.addHandler(logging.NullHandler())

def activate_debug_logging():
    # define a Handler which writes DEBUG messages to sys.stderr
    debugger = logging.StreamHandler()
    debugger.setLevel(logging.DEBUG)
    debugger.addFilter(DebugFilter())
    formatter = logging.Formatter('%(levelname)-8s %(message)s -- Module: %(module)s in %(funcName)s, line %(lineno)d')
    debugger.setFormatter(formatter)
    log.addHandler(debugger)

def activate_console_logging():
    # define a Handler which writes INFO messages or higher to the sys.stderr
    console = logging.StreamHandler()
    console.setLevel(LOGGING_LEVEL)
    formatter = logging.Formatter('%(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    log.addHandler(console)

def activate_file_logging(file_name='condorpy.log'):
    # define a Handler which logs all messages to a file
    log_file = logging.FileHandler(file_name, mode='w')
    log_file.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    log_file.setFormatter(formatter)
    log.addHandler(log_file)