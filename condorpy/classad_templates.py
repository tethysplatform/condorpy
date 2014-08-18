'''
Created on Aug 4, 2014

@author: sdc50
'''

try:
    import classad
except:
    import pseudoclassad as classad



#Spencer Taylor job for running GSSHA
ST_GSSHA = classad.ClassAd({
    'JobUniverse' : 5, #Vanilla
    'Requirements' : '( Arch == \'X86_64\' && OpSys == \'WINDOWS\' )', #?
    'RequestMemory' : 1200,
    'Out' : 'logs/..out',
    'Err' : 'logs/..err',
    'UserLog' : 'logs/condor.log',
    'ShouldTransferFiles' : 'YES',
    'WhenToTransferOutput' : 'ON_EXIT_OR_EVICT'
    })

'''
'Universe     = vanilla\n')
'Executable   = {0}\n').format(exe))
'Requirements = Arch == 'X86_64' && OpSys == 'WINDOWS'\n')
'Request_Memory = 1200 Mb\n')
'Log          = {0}log\n').format('logs/condor.'))
'Output       = {0}out\n').format('logs/$(cluster).$(process).'))
'Error        = {0}err\n').format('logs/$(cluster).$(process).'))
'Arguments    = {0}\n').format(prj + ' $(process)'))
'transfer_executable     = TRUE\n')
'transfer_input_files    = ' + ','.join(['../' + f for f in fileList]) + '\n')
'should_transfer_files   = YES\n')
'transfer_output_files   = {0}\n').format(outputDir))
'when_to_transfer_output = ON_EXIT_OR_EVICT\n')
'Initialdir        = {0}\n').format(resultsFilePath))
'Queue {0}'.format(numJobs))
        

Requirements = ( Arch == 'X86_64' && OpSys == 'WINDOWS' ) && ( TARGET.Disk >= RequestDisk ) && ( TARGET.Memory >= RequestMemory ) && ( TARGET.HasFileTransfer )
RequestMemory = 1200
Out = 'logs/..out'
TransferOutput = 'Stochastic_Output/'
WhenToTransferOutput = 'ON_EXIT_OR_EVICT'
Err = 'logs/..err'
UserLog = 'C:\Users\htcondor\Desktop\StochasticGSSHA\Ben_79/Results1/logs/condor.log'
Iwd = 'C:\Users\htcondor\Desktop\StochasticGSSHA\Ben_79/Results1/'
JobUniverse = 5
TransferInput = '../FinalRuns.cif,../FinalRuns.cmt,../FinalRuns.ele,../FinalRuns.gag,../FinalRuns.gst,../FinalRuns.ihl,../FinalRuns.lsf,../FinalRuns.map,../FinalRuns.msk,../FinalRuns.prj,../FinalRuns.smt,../FinalRuns_prj.pro,../gssha62.exe,../Land_Use.idx,../Soil_Type.idx,../Uniform.idx'
Cmd = 'C:\Users\htcondor\Desktop\StochasticGSSHA\Ben_79/runStochasticGSSHA.py'
TargetType = 'Machine'
ShouldTransferFiles = 'YES'
'''
