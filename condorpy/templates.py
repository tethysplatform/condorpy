'''
Created on Aug 4, 2014

@author: sdc50
'''
from collections import OrderedDict

class Templates(object):
    """

    """

    def __init__(self):
        pass

    @property
    def base(self):
        base = OrderedDict()
        base['job_name'] = ''
        base['universe'] = ''
        base['executable'] = ''
        base['arguments'] = ''
        base['initialdir'] = '$(job_name)'
        base['logdir'] = 'logs'
        base['log'] = '$(logsdir)/$(job_name).$(cluster).log'
        base['output'] = '$(logdir)/$(job_name).$(cluster).$(process).out'
        base['error'] = '$(logdir)/$(job_name).$(cluster).$(process).err'
        return base

    @property
    def vanilla_base(self):
        vanilla_base = self.base
        vanilla_base['universe'] = 'vanilla'
        return vanilla_base

    @property
    def vanilla_transfer_files(self):
        vanilla_transfer_files = self.vanilla_base
        vanilla_transfer_files['transfer_executable'] = 'TRUE'
        vanilla_transfer_files['should_transfer_files'] = 'YES'
        vanilla_transfer_files['when_to_transfer_output'] = 'ON_EXIT_OR_EVICT'
        vanilla_transfer_files['transfer_input_files'] = ''
        vanilla_transfer_files['transfer_output_files'] = ''
        return vanilla_transfer_files

    @property
    def vanilla_nfs(self):
        vanilla_nfs = self.vanilla_base
        #TODO: test nfs jobs
        return vanilla_nfs