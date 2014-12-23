from unittest import TestCase
from condorpy import Job, DAG, Node, Templates
import os

__author__ = 'sdc50'


class TestTemplates(TestCase):

    def setUp(self):
        self.default_save_location = os.path.join(os.getcwd(), '../condorpy/condorpy-saved-templates')
        self.custom_save_location = 'saved-templates'

    def tearDown(self):
        for file in (self.custom_save_location, self.default_save_location):
            if os.path.isfile(file):
                os.remove(file)

    def test__init__(self):
        pass

    def test__str__(self):
        pass

    def test__repr__(self):
        pass

    def test___getattribute__(self):
        custom = dict(key='value')
        Templates.custom = custom
        new = Templates.custom
        self.assertIsNot(custom, new)

    def test_save(self):
        file = self.default_save_location
        Templates.save()#self.saved_templates_file)
        f = open(file)
        expected = "(dp0\nS'custom'\np1\n(dp2\nS'key'\np3\nS'value'\np4\nss."
        actual = f.read()
        msg = 'testing saving with empty dict'
        self.assertEqual(expected, actual, '%s\nExpected: %s\nActual:   %s\n' % (msg, expected, actual))

