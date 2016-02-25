*************
Creating Jobs
*************

The CondorPy Job object represents an HTCondor job (which is sometimes called a cluster; see `Sub-Jobs`_). A CondorPy Job automatically creates an HTCondor submit description file (or ``job_file``). CondorPy seeks to simplify the process of creating HTCondor jobs as much as possible, but it is still helpful to understand how submit description files work. For a good overview see `Submitting a Job <http://research.cs.wisc.edu/htcondor/manual/current/2_5Submitting_Job.html>`_ in the HTCondor Users' Manual.

Creating a new CondorPy Job object is very easy and only requires a name:

::

    from condorpy import Job

    job = Job(name='my_job')

The job can then be configured by setting properties and attributes (see `Job Properties`_ and `Setting Job Attributes`_). For convenience the Job constructor can also take a number of other arguments, which help to configure the job when it is created. For example:

::

    from condorpy import Job, Templates

    job = Job(name='my_job',
              attributes=Templates.base,
              num_jobs=5,
              host='example.com',
              username='root',
              private_key='~/.ssh/id_rsa',
              executable='my_script.py',
              arguments=('arg1', 'arg2')
              )

Here is a brief explanation of the arguments used in this example with links for additional details:

    * ``attributes``: A dictionary of job attributes. In this case a Template is used (see :doc:`using_templates`).
    * ``num_jobs``: The number of sub-jobs that will be created as part of the job (see `Sub-Jobs`_).
    * ``host``: The hostname or IP address of a remote scheduler where the job will be submitted (see :doc:`remote_scheduling`).
    * ``username``: The username for the remote scheduler (see :doc:`remote_scheduling`).
    * ``private_key``: The path the the private SSH key used to connect to the remote scheduler (see :doc:`remote_scheduling`).
    * ``**kwargs``: A list of keyword arguments (in this example ``executable`` and ``arguments``) that will be added to the attributes dictionary (see `Setting Job Attributes`_).

For a full list of arguments that can be used in the Job constructor see the :doc:`../modules`.

Job Properties
==============
Jobs have the following properties (some of which may be set and others are read only):

    * ``name`` (str): The name of the job. This is used to name the ``job_file``, the ``log_file``, and the ``initial_directory``. The ``job_name`` job attribute in the ``attributes`` dictionary is also set by the ``name`` property.
    * ``attributes`` (dict, read_only): A list of job attributes and their values. This property can be set in the Job constructor and can be modified by setting individual job attributes (see `Setting Job Attributes`_).
    * ``num_jobs`` (int): The number of sub-jobs that are part of the job. This can also be set when submitting the job (see `Sub-Jobs`_).
    * ``cluster_id`` (int, read_only): The id used to identify the job on the HTCondor scheduler.
    * ``status`` (str, read_only): The status of the job.
    * ``statuses`` (dict, read_only): A dictionary where the keys are all possible statuses and the values are the number of sub-jobs that have that status. The possible statuses are:

        - 'Unexpanded'
        - 'Idle'
        - 'Running'
        - 'Removed'
        - 'Completed'
        - 'Held'
        - 'Submission_err'

    * ``job_file`` (str, read_only): The file path to the job file in the form [``initial_directory``]/[``name``].job
    * ``log_file`` (str, read_only): The file path to the main log file and by default is [``initial_directory``]/logs/[``name``].log
    * ``initial_directory`` (str, read_only): The home directory for the job. All input files are relative to this directory and all output files are written back to this directory (see :doc:`file_paths`). By default the directory is created in the current working directory and is called [``name``]. This property comes from the ``initialdir`` job attribute.
    * ``remote_input_files`` (list or tuple): A list or tuple of file paths to files that need to be transfered to a remote scheduler (see :doc:`remote_scheduling`).

Setting Job Attributes
======================
Job attributes are key-value pairs that get written to the job file (i.e. the HTCondor submit description file). These attributes can be set in four different ways:
    1. Using the ``attributes`` parameter in the Job constructor.
    2. Using ``**kwargs`` in the Job constructor.
    3. Using the ``set`` method of a job object.
    4. Assigning values to attributes directly on a job object.

Valid job attributes are any of the commands that can be listed in the HTCondor submit description file. For a complete list and description of these commands see the `HTCondor condor_submit documentation <http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html>`_.

Using the ``attributes`` parameter in the Job constructor
---------------------------------------------------------
The ``attributes`` parameter in the Job constructor accepts a dictionary, which becomes the attributes of the newly created job. This is often used to pass is a template that has pre-configured attributes, but it can be any dictionary object.

The following example uses a template to initialize a job with several job attributes using the ``attributes`` parameter of the Job constructor.

::

    from condorpy import Job, Templates

    job = Job(name='my_job', attributes=Templates.base)


This next example modifies a template to initialize a job with customized job attributes.

::

    from condorpy import Job, Templates

    my_attributes = Templates.base
    my_attributes['executable'] = 'my_script.py'
    my_attributes['arguments'] = ('arg1', 'arg2')

    job = Job(name='my_job', attributes=my_attributes


Using ``**kwargs`` in the Job constructor
-----------------------------------------
Additional job attributes can be set in the Job constructor by using keyword arguments (or kwargs).

In the following example ``executable`` and ``arguments`` are keyword arguments that get added as job attributes.

::

    from condorpy import Job, Templates

    job = Job(name='my_job', attributes=Templates.base, executable='my_script.py', arguments=('arg1', 'arg2'))

Using the ``set`` method of a job object
----------------------------------------
Once an object has been instantiated from the Job class then attributes can be set using the ``set`` method.

In this example the ``executable`` and ``arguments`` attributes are set after the job has been created.

::

    from condorpy import Job, Templates

    job = Job(name='my_job', attributes=Templates.base)
    job.set('executable', 'my_script.py')
    job.set('arguments', ('arg1', 'arg2'))

Assigning values to attributes directly on a job object
-------------------------------------------------------
For convenience job attributes can be assigned directly on the job object.

In the following example the ``executable`` and ``arguments`` attributes are set as attributes on the job object.

::

    from condorpy import Job, Templates

    job = Job(name='my_job', attributes=Templates.base)
    job.executable = 'my_script.py'
    job.arguments = ('arg1', 'arg2')

Sub-Jobs
========
It is often useful to have a single job execute multiple times with different arguments, or input data. This is what HTCondor calls a cluster of multiple jobs. In CondorPy it is said that the job has multiple sub-jobs. Creating multiple sub-jobs in CondorPy can be done in two ways: setting the ``num_jobs`` property of the job, or passing in a ``queue`` argument to the ``submit`` method, which also sets the ``nubm_jobs`` property.

::

    # creating 100 sub-jobs by setting the num_jobs property
    job.num_jobs = 100

    # creating 100 sub-jobs by passing in a queue argument to the submit method
    job.submit(queue=100)



Working Directory
=================


