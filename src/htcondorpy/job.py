'''
Created on May 23, 2014

@author: sdc50
'''

class Job(Object):
    '''
    classdocs
    http://research.cs.wisc.edu/htcondor/manual/v7.8/condor_submit.html#man-condor-submit
    
    '''
    

    def __init__(self, exe):
        '''
        Constructor
        '''
        self.universe= 'vanilla'
        self.executable = exe
        self.requirements = 'Arch == "X86_64" && OpSys == "WINDOWS"'
        
        
        
    def writeJob(jobFile,numJobs,exe,projectFile,resultsFilePath):
        projectFilePath = os.path.dirname(projectFile)
        prj = os.path.basename(projectFile)
        fileList = fileFinder(projectFile)
        
        #'\n'.join('%s\t=\t%s' % (key, val) for (key,val) in dict.items())
        
    
        with open(jobFile, 'w') as file:
            file.write("Universe     = vanilla\n")
            file.write(("Executable   = {0}\n").format(exe))
            file.write('Requirements = Arch == "X86_64" && OpSys == "WINDOWS"\n')
            file.write("Request_Memory = 1200 Mb\n")
            file.write(("Log          = {0}log\n").format('logs/condor.'))
            file.write(("Output       = {0}out\n").format('logs/$(cluster).$(process).'))
            file.write(("Error        = {0}err\n").format('logs/$(cluster).$(process).'))
            file.write(("Arguments    = {0}\n").format(prj + ' $(process)'))
            file.write("transfer_executable     = TRUE\n")
            file.write("transfer_input_files    = " + ",".join(['../' + f for f in fileList]) + "\n")
            file.write("should_transfer_files   = YES\n")
            file.write("when_to_transfer_output = ON_EXIT_OR_EVICT\n")
            file.write(("Initialdir        = {0}\n").format(resultsFilePath))
            file.write("Queue {0}".format(numJobs))     
            
            