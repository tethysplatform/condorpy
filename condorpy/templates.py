# Copyright (c) 2015 Scott Christensen
#
# This file is part of condorpy
#
# condorpy is free software: you can redistribute it and/or modify it under
# the terms of the BSD 2-Clause License. A copy of the BSD 2-Clause License
# should have been distributed with this file.

from collections import OrderedDict
from copy import deepcopy
import pickle, os

class Templates(object):
    """

    """

    def __init__(self):
        pass

    def __getattribute__(self, item):
        """

        :param item:
        :return:
        """
        attr = object.__getattribute__(self, item)
        if item in object.__getattribute__(self, '__dict__'):
            attr = deepcopy(attr)
        return attr

    def save(self, file_name=None):
        if not file_name:
            file_name = os.path.join(os.path.dirname(__file__), 'condorpy-saved-templates')
        with open(file_name, 'wb') as file:
            pickle.dump(self.__dict__, file, protocol=0)

    def load(self, file_name=None):
        if not file_name:
            file_name = os.path.join(os.path.dirname(__file__), 'condorpy-saved-templates')
        if not os.path.isfile(file_name):
            return
            #TODO: raise an error? log warning?
        with open(file_name, 'rb') as file:
            pdict = pickle.load(file)
        self.__dict__.update(pdict)

    def reset(self):
        self.__dict__ = dict()

    @property
    def base(self):
        base = OrderedDict()
        base['job_name'] = ''
        base['universe'] = ''
        base['executable'] = ''
        base['arguments'] = ''
        base['initialdir'] = '$(job_name)'
        base['logdir'] = 'logs'
        base['log'] = '$(logdir)/$(job_name).$(cluster).log'
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
        return vanilla_nfs