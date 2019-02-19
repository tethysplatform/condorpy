========
CondorPy
========
:condorpy: Python interface for high throughput computing with HTCondor
:Version: 0.5.0
:Author: Scott Christensen
:Team: Tethys Platform
:Homepage: http://tethysplatform.org/condorpy/
:License: BSD 2-Clause

Description:
============
Condorpy is a wrapper for the command line interface (cli) of HTCondor and enables creating submitting and monitoring HTCondor jobs from Python. HTCondor must be installed to use condorpy.

Installing:
===========
::

    $ pip install condorpy


Code Example:
=============
::

    >>> from condorpy import Job, Templates
    >>> job = Job('job_name', Templates.vanilla_transfer_files)
    >>> job.executable = 'job_script'
    >>> jobs.arguments = 'input_1 input_2'
    >>> job.transfer_input_files = 'input_1 input_2'
    >>> job.transfer_output_files = 'output'
    >>> job.submit()

