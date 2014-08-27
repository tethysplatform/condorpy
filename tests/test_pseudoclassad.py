'''
Created on Aug 22, 2014

@author: sdc50
'''
#TODO: look into refactoring this module into separate classes to mirror the objects being tested

import unittest
from condorpy import pseudoclassad

def testSuite():
    return unittest.TestLoader().loadTestsFromTestCase(TestPseudoclassad)

class TestPseudoclassad(unittest.TestCase):


    def setUp(self):
        self.ad = pseudoclassad.ClassAd()
        self.test_dict = {'Foo':'Bar'}
        self.test_ad = pseudoclassad.ClassAd(self.test_dict)


    def tearDown(self):
        self.ad = None
        self.test_dict = None
        self.test_ad = None


    def test_parse(self):
        '''
        Parse input into a ClassAd. Returns a ClassAd object.

        Parameter input is a string-like object or a file pointer.
        '''
        self.assertRaises(NotImplementedError, pseudoclassad.parse, '')

    def test_parseOld(self):
        '''
        Parse old ClassAd format input into a ClassAd.
        '''
        self.assertRaises(NotImplementedError, pseudoclassad.parseOld, '')

    def test_version(self):
        '''
        Return the version of the linked ClassAd library.
        '''
        self.assertRaises(NotImplementedError, pseudoclassad.version)

    def test_classad_init(self):
        '''
        Create a ClassAd object from string, str, passed as a parameter.
        The string must be formatted in the new ClassAd format.
        '''

        #test instantiation of attributes
        expected = self.ad.attributes
        actual = dict
        self.assertIsInstance(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #test dictionary argument instantiation
        expected = self.test_dict
        actual = self.test_ad.attributes
        self.assertIs(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

    def test_classad_len(self):
        '''
        Returns the number of attributes in the ClassAd; allows len(object) semantics for ClassAds.
        '''

        #test length function of add with 1 item
        expected = len(self.test_dict)
        actual = len(self.test_ad)
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #test length of empty ad
        expected = 0
        actual = len(self.ad)
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

    def test_classad_str(self):
        '''
        Converts the ClassAd to a string and returns the string; the formatting style is new ClassAd,
        with square brackets and semicolons. For example, [ Foo = "bar"; ] may be returned.
        '''

        #test printing of empty ad
        expected = '[  ]'
        actual = str(self.ad)
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #test printing of ad with 1 attribute
        expected = '[ Foo = Bar; ]'
        actual = str(self.test_ad)
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #test printing of ad with multiple attributes
        new_ad = pseudoclassad.ClassAd({'Foo':'Bar','Fruit':'Orange'})
        expected = '[\n  Foo = Bar;\n  Fruit = Orange;\n]'
        actual = str(new_ad)
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

    def test_classad_repr(self):
        #test printing of empty ad
        expected = '[  ]'
        actual = self.ad.__repr__()
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #test printing of ad with 1 attribute
        expected = '[ Foo = Bar; ]'
        actual = self.test_ad.__repr__()
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #test printing of ad with multiple attributes
        new_ad = pseudoclassad.ClassAd({'Foo':'Bar','Fruit':'Orange'})
        expected = '[\n  Foo = Bar;\n  Fruit = Orange;\n]'
        actual = new_ad.__repr__()
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

    def test_classad_items(self):
        '''
        Returns an iterator of tuples. Each item returned by the iterator is a tuple representing a
        pair (attribute,value) in the ClassAd object.
        '''
        self.assertRaises(NotImplementedError, self.ad.items)

    def test_classad_values(self):
        '''
        Returns an iterator of objects. Each item returned by the iterator is a value in the ClassAd.

        If the value is a literal, it will be cast to a native Python object, so a ClassAd string will be
        returned as a Python string.
        '''

        self.assertRaises(NotImplementedError, self.ad.values)

    def test_classad_keys(self):
        '''
        Returns an iterator of strings. Each item returned by the iterator is an attribute string in the ClassAd.
        '''

        self.assertRaises(NotImplementedError, self.ad.keys)

    def test_classad_get(self):
        '''
        Behaves like the corresponding Python dictionary method. Given the attr as key, returns either
        the value of that key, or if the key is not in the object, returns None or the optional second
        parameter when specified.
        '''


        #testing none is returned from empty ad
        actual = self.ad.get('Foo')
        self.assertIsNone(actual, 'expected: None, actual: %s' % (actual))

        #getting existing attribute
        expected = 'Bar'
        actual = self.test_ad.get('Foo')
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #testing non-existing attribute on non-empty ad
        actual = self.test_ad.get('FooBar')
        self.assertIsNone(actual, 'expected: None, actual: %s' % (actual))

    def test_classad_getitem(self):
        '''
        Returns (as an object) the value corresponding to the attribute attr passed as a parameter.

        ClassAd values will be returned as Python objects; ClassAd expressions will be returned as ExprTree objects.
        '''

        #testing none is returned from empty ad
        actual = self.ad.__getitem__('Foo')
        self.assertIsNone(actual, 'expected: None, actual: %s' % (actual))

        #getting existing attribute
        expected = 'Bar'
        actual = self.test_ad.__getitem__('Foo')
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #testing non-existing attribute on non-empty ad
        actual = self.test_ad.__getitem__('FooBar')
        self.assertIsNone(actual, 'expected: None, actual: %s' % (actual))

    def test_classad_setitem(self):
        '''
        Sets the ClassAd attribute attr to the value.

        ClassAd values will be returned as Python objects; ClassAd expressions will be returned as ExprTree objects.
        '''

        attr = 'Fruit'

        #test setting attribute
        expected = 'Orange'
        self.ad.__setitem__(attr, expected)
        actual = self.ad.get(attr)
        self.assertEqual(expected, actual, 'expected: %s, actual: %s' % (expected, actual))

        #test resetting attribute
        msg = 'resetting attribute failed -'
        expected = 'Apple'
        self.ad.__setitem__(attr, expected)
        actual = self.ad.get(attr)
        self.assertEqual(expected, actual, '%s expected: %s, actual: %s' % (msg, expected, actual))

    def test_classad_setdefault(self):
        '''
        Behaves like the corresponding Python dictionary method. If called with
        an attribute, attr, that is not set, it will set the attribute to the
        specified value. It returns the value of the attribute. If called with
        an attribute that is already set, it does not change the object.
        '''

        self.assertRaises(NotImplementedError, self.ad.setdefault, None, None)

    def test_classad_eval(self):
        '''
        Evaluate the value given a ClassAd attribute attr. Throws ValueError if unable to evaluate the object.

        Returns the Python object corresponding to the evaluated ClassAd attribute.
        '''

        self.assertRaises(NotImplementedError, self.ad.eval,None)

    def test_classad_lookup(self):
        '''
        Look up the ExprTree object associated with attribute attr. No attempt will be made to convert to a Python object.

        Returns an ExprTree object.
        '''

        self.assertRaises(NotImplementedError, self.ad.lookup, None)

    def test_classad_printOld(self):
        '''
        Print the ClassAd in the old ClassAd format.

        Returns a string.
        '''

        self.assertRaises(NotImplementedError, self.ad.printOld)


    def test_expresion_tree__init__(self):
        '''
        Parse the string str to create an ExprTree.
        '''

        #test constructor
        expected = pseudoclassad.ExprTree
        actual = pseudoclassad.ExprTree()
        self.assertIsInstance(actual, expected)

    def test_expresion_tree__str__(self):
        '''
        Represent and return the ClassAd expression as a string.
        '''

        expr_tree = pseudoclassad.ExprTree()
        self.assertRaises(NotImplementedError, expr_tree.__str__)

    def test_expresion_tree_eval(self):
        '''
        Evaluate the expression and return as a ClassAd value, typically a Python object.
        '''

        expr_tree = pseudoclassad.ExprTree()
        self.assertRaises(NotImplementedError, expr_tree.eval)


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()