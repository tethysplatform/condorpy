'''
Created on Aug 4, 2014

@author: sdc50
'''
from copy import deepcopy

base = {
    'initialdir':'',
    'logdir':'logs',
    'log':'$(logsdir)/$(cluster).log',
    'output':'$(logdir)/$(cluster).$(process).out',
    'error':'$(logdir)/$(cluster).$(process).err',
}

@property
def base():
    return deepcopy(base)


vanilla_base = base()
vanilla_base['universe'] = 'vanilla'

@property
def vanilla_base():
    return deepcopy(vanilla_base)


vanilla_transfer_files = vanilla_base()
vanilla_transfer_files['transfer_executable'] = 'TRUE'
vanilla_transfer_files['should_transfer_files'] = 'YES'
vanilla_transfer_files['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
vanilla_transfer_files['transfer_input_files'] = ''
vanilla_transfer_files['transfer_output_files'] = ''

@property
def vanilla_trasfer_files():
    return deepcopy(vanilla_transfer_files)

vanilla_nfs = vanilla_base()
#TODO: test nfs jobs

@property
def vanilla_nfs():
    return deepcopy(vanilla_nfs)