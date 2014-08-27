'''
Created on Jul 30, 2014

@author: sdc50
'''

##########################################
#
#    Module Functions
#
##########################################

def parse(input):
    '''
    Parse input into a ClassAd. Returns a ClassAd object.

    Parameter input is a string-like object or a file pointer.
    '''
    raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
def parseOld(input):
    '''
    Parse old ClassAd format input into a ClassAd.
    '''
    raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
def version():
    '''
    Return the version of the linked ClassAd library.
    '''
    raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")

##########################################
#
#    ClassAd Class
#
##########################################
    
class ClassAd(object):
    '''
    
    '''
    
    def __init__(self, str=None):
        '''
        Create a ClassAd object from string, str, passed as a parameter. 
        The string must be formatted in the new ClassAd format.
        '''
        
        if str != None:
            self.attributes = str
        else:
            self.attributes = dict()
        
        #raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def __len__(self):
        '''
        Returns the number of attributes in the ClassAd; allows len(object) semantics for ClassAds.
        '''
        
        return len(self.attributes)
        
        #raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def __str__(self):
        '''
        Converts the ClassAd to a string and returns the string; the formatting style is new ClassAd, 
        with square brackets and semicolons. For example, [ Foo = "bar"; ] may be returned.
        '''
        
        list = []
        for key,value in self.attributes.iteritems():
            list.append(key + ' = ' + str(value) + ';')
        if len(list) > 1:
            ad = '[\n  ' + '\n  '.join(list) + '\n]'
        else:
            ad = '[ ' + ''.join(list) + ' ]'
        return ad
        
        #raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def __repr__(self):
        return self.__str__()
    
    def items(self):
        '''
        Returns an iterator of tuples. Each item returned by the iterator is a tuple representing a 
        pair (attribute,value) in the ClassAd object.
        '''
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def values(self):
        '''
        Returns an iterator of objects. Each item returned by the iterator is a value in the ClassAd.

        If the value is a literal, it will be cast to a native Python object, so a ClassAd string will be 
        returned as a Python string.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def keys(self):
        '''
        Returns an iterator of strings. Each item returned by the iterator is an attribute string in the ClassAd.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def get(self, attr, value=None):
        '''
        Behaves like the corresponding Python dictionary method. Given the attr as key, returns either 
        the value of that key, or if the key is not in the object, returns None or the optional second 
        parameter when specified.
        '''
        return self.attributes.get(attr, value)
    
        #raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def __getitem__(self, attr):
        '''
        Returns (as an object) the value corresponding to the attribute attr passed as a parameter.

        ClassAd values will be returned as Python objects; ClassAd expressions will be returned as ExprTree objects.
        '''
        
        return self.attributes.get(attr)
    
        #raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def __setitem__(self, attr, value):
        '''
        Sets the ClassAd attribute attr to the value.

        ClassAd values will be returned as Python objects; ClassAd expressions will be returned as ExprTree objects.
        '''
        
        self.attributes[attr] = value
        #raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def setdefault(self, attr, value):
        '''
        Behaves like the corresponding Python dictionary method. If called with 
        an attribute, attr, that is not set, it will set the attribute to the 
        specified value. It returns the value of the attribute. If called with 
        an attribute that is already set, it does not change the object.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def eval(self, attr):
        '''
        Evaluate the value given a ClassAd attribute attr. Throws ValueError if unable to evaluate the object.

        Returns the Python object corresponding to the evaluated ClassAd attribute.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def lookup(self, attr):
        '''
        Look up the ExprTree object associated with attribute attr. No attempt will be made to convert to a Python object.

        Returns an ExprTree object.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def printOld(self):
        '''
        Print the ClassAd in the old ClassAd format.

        Returns a string.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    

class ExprTree(object):
    '''
    '''
    
    def __init__(self):
        '''
        Parse the string str to create an ExprTree.
        '''
        pass
    
    def __str__(self):
        '''
        Represent and return the ClassAd expression as a string.
        '''
        
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")
    
    def eval(self):
        '''
        Evaluate the expression and return as a ClassAd value, typically a Python object.
        '''
        raise NotImplementedError("This method has not been implemented in the unofficial version of htcondor classad")