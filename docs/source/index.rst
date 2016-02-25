.. condorpy documentation master file, created by
   sphinx-quickstart on Mon Mar 16 16:08:02 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

********
CondorPy
********
:condorpy: Python interface for high-throughput computing with HTCondor
:Version: 0.0.0
:Author: Scott Christensen
:Team: CI-Water
:Homepage: http://tethysplatform.org/condorpy
:License: BSD 2-Clause

Contents:

.. toctree::
   :maxdepth: 3

   user_manual
   htcondor
   modules


Description
===========
Condorpy is a wrapper for the command line interface (cli) of HTCondor and enables creating submitting and monitoring HTCondor jobs from Python. HTCondor must be installed to use condorpy.

Installing
==========
::

    $ pip install condorpy

Installing from Source
======================
::

    $ python setup.py install


Getting Started
===============
::

    >>> from condorpy import Job, Templates
    >>> job = Job('job_name', Templates.vanilla_transfer_files)
    >>> job.executable = 'job_script'
    >>> jobs.arguments = 'input_1 input_2'
    >>> job.transfer_input_files = 'input_1 input_2'
    >>> job.transfer_output_files = 'output'
    >>> job.submit()

TODO
====

Acknowledgements
================

This material was developed as part of the `CI-Water project <http://ci-water.org/>`_ which was supported by the National Science Foundation under Grant No. 1135482

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

