'''
Created on Aug 4, 2014

@author: sdc50
'''
from copy import deepcopy

_base = {
    'initialdir':'',
    'logdir':'logs',
    'log':'$(logsdir)/$(cluster).log',
    'output':'$(logdir)/$(cluster).$(process).out',
    'error':'$(logdir)/$(cluster).$(process).err',
}


def base():
    return deepcopy(_base)


_vanilla_base = base()
_vanilla_base['universe'] = 'vanilla'


def vanilla_base():
    return deepcopy(_vanilla_base)


_vanilla_transfer_files = vanilla_base()
_vanilla_transfer_files['transfer_executable'] = 'TRUE'
_vanilla_transfer_files['should_transfer_files'] = 'YES'
_vanilla_transfer_files['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
_vanilla_transfer_files['transfer_input_files'] = ''
_vanilla_transfer_files['transfer_output_files'] = ''


def vanilla_trasfer_files():
    return deepcopy(_vanilla_transfer_files)

vanilla_nfs = vanilla_base()
#TODO: test nfs jobs

@property
def vanilla_nfs():
    return deepcopy(vanilla_nfs)