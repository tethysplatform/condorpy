from setuptools import setup, find_packages

setup(
    name = "condorpy",
    version = "0.0.0",
    packages = find_packages(),
    #scripts = ['say_hello.py'],

    # Project uses reStructuredText, so ensure that the docutils get
    # installed or upgraded on the target machine
    #install_requires = ['docutils>=0.3'],
    

#    package_data = {
#        # If any package contains *.txt or *.rst files, include them:
#        '': ['*.txt', '*.rst'],
#        # And include any *.msg files found in the 'hello' package, too:
#        'hello': ['*.msg'],
#    },

    
    # metadata for upload to PyPI
    author = "Scott Christensen",
    author_email = "sdc50@byu.net",
    description = "A package for creating and submitting jobs to HTCondor",
    license = "PSF",
    keywords = "htcondor distributed-computing job-scheduling",
    url = "https://bitbucket.org/sdc50/condorpy/wiki/Home",   # project home page, if any

    # could also include long_description, download_url, classifiers, etc.
)