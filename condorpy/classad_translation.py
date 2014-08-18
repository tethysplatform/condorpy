'''
Created on Aug 4', ''2014

@author: sdc50
'''
import re

ATTR_TRANSLATION_LIST = [
    # ( 'Absent', '' ),
    # ( 'AcctGroup', '' ),
    # ( 'AcctGroupUser', '' ),
    # ( 'AllRemoteHosts', '' ),
    # ( 'Args', 'arguments' ),    #old syntax
      ( 'Arguments', 'arguments' ), #new syntax
    # ( 'BatchQueue', '' ),
    # ( 'BoincAuthenticatorFile', '' ),
    # ( 'CkptArch', '' ),
    # ( 'CkptOpSys', '' ),
    # ( 'ClusterId', '' ),
      ( 'Cmd', 'executable' ),
    # ( 'CommittedTime', '' ),
    # ( 'CommittedSlotTime', '' ),
    # ( 'CommittedSuspensionTime', '' ),
    # ( 'CompletionDate', '' ),
    # ( 'ConcurrencyLimits', '' ),
    # ( 'CumulativeSlotTime', '' ),
    # ( 'CumulativeSuspensionTime', '' ),
    # ( 'CumulativeTransferTime', '' ),
    # ( 'CurrentHosts', '' ),
    # ( 'DAGManJobId', '' ),
    # ( 'DAGParentNodeNames', '' ),
    # ( 'DAGManNodesLog', '' ),
    # ( 'DAGManNodesMask', '' ),
    # ( 'DelegateJobGSICredentialsLifetime', '' ),
    # ( 'DeltacloudAvailableActions', '' ),
    # ( 'DeltacloudHardwareProfile', '' ),
    # ( 'DeltacloudHardwareProfileCpu', '' ),
    # ( 'DeltacloudHardwareProfileMemory', '' ),
    # ( 'DeltacloudHardwareProfileStorage', '' ),
    # ( 'DeltacloudImageId', '' ),
    # ( 'DeltacloudKeyname', '' ),
    # ( 'DeltacloudPasswordFile', '' ),
    # ( 'DeltacloudPrivateNetworkAddresses', '' ),
    # ( 'DeltacloudPublicNetworkAddresses', '' ),
    # ( 'DeltacloudRealmId', '' ),
    # ( 'DeltacloudUserData', '' ),
    # ( 'DeltacloudUsername', '' ),
    # ( 'DiskUsage', '' ),
    # ( 'EC2AccessKeyId', '' ),
    # ( 'EC2AmiID', '' ),
    # ( 'EC2ElasticIp', '' ),
    # ( 'EC2InstanceName', '' ),
    # ( 'EC2InstanceType', '' ),
    # ( 'EC2KeyPair', '' ),
    # ( 'EC2SpotPrice', '' ),
    # ( 'EC2SpotRequestID', '' ),
    # ( 'EC2StatusReasonCode', '' ),
    # ( 'EC2TagNames', '' ),
    # ( 'EC2KeyPairFile', '' ),
    # ( 'EC2RemoteVirtualMachineName', '' ),
    # ( 'EC2SecretAccessKey', '' ),
    # ( 'EC2SecurityGroups', '' ),
    # ( 'EC2UserData', '' ),
    # ( 'EC2UserDataFile', '' ),
      ( 'Err', 'error' ),
    # ( 'EmailAttributes', '' ),
    # ( 'EnteredCurrentStatus', '' ),
    # ( 'ExecutableSize', '' ),
    # ( 'ExitBySignal', '' ),
    # ( 'ExitCode', '' ),
    # ( 'ExitSignal', '' ),
    # ( 'ExitStatus', '' ),
    # ( 'GceAuthFile', '' ),
    # ( 'GceImage', '' ),
    # ( 'GceMachineType', '' ),
    # ( 'GceMetadata', '' ),
    # ( 'GceMetadataFile', '' ),
    # ( 'GridJobStatus', '' ),
    # ( 'GridResource', '' ),
    # ( 'HoldKillSig', '' ),
    # ( 'HoldReason', '' ),
    # ( 'HoldReasonCode', '' ),
    # ( 'HoldReasonSubCode', '' ),
    # ( 'HookKeyword', '' ),
    # ( 'ImageSize', '' ),
      ( 'In', 'input' ),
      ( 'Iwd', 'initialdir' ),
    # ( 'IwdFlushNFSCache', '' ),
    # ( 'JobAdInformationAttrs', '' ),
    # ( 'JobDescription', '' ),
    # ( 'JobCurrentStartDate', '' ),
    # ( 'JobCurrentStartExecutingDate', '' ),
    # ( 'JobCurrentStartTransferOutputDate', '' ),
    # ( 'JobLeaseDuration', '' ),
    # ( 'JobMaxVacateTime', '' ),
    # ( 'JobNotification', '' ),
    # ( 'JobPrio', '' ),
    # ( 'JobRunCount', '' ),
    # ( 'JobStartDate', '' ),
    # ( 'JobStatus', '' ),
      ( 'JobUniverse', 'universe' ),
    # ( 'KeepClaimIdle', '' ),
    # ( 'KillSig', '' ),
    # ( 'KillSigTimeout', '' ),
    # ( 'LastCheckpointPlatform', '' ),
    # ( 'LastCkptServer', '' ),
    # ( 'LastCkptTime', '' ),
    # ( 'LastMatchTime', '' ),
    # ( 'LastRejMatchReason', '' ),
    # ( 'LastRejMatchTime', '' ),
    # ( 'LastRemotePool', '' ),
    # ( 'LastSuspensionTime', '' ),
    # ( 'LastVacateTime', '' ),
    # ( 'LeaveJobInQueue', '' ),
    # ( 'LocalSysCpu', '' ),
    # ( 'LocalUserCpu', '' ),
    # ( 'MachineAttr&lt;X&gt;&lt;N&gt;', '' ),
    # ( 'MaxHosts', '' ),
    # ( 'MaxJobRetirementTime', '' ),
    # ( 'MaxTransferInputMB', '' ),
    # ( 'MaxTransferOutputMB', '' ),
    # ( 'MemoryUsage', '' ),
    # ( 'MinHosts', '' ),
    # ( 'NextJobStartDelay', '' ),
    # ( 'NiceUser', '' ),
    # ( 'Nonessential', '' ),
    # ( 'NTDomain', '' ),
    # ( 'NumCkpts', '' ),
    # ( 'NumGlobusSubmits', '' ),
    # ( 'NumJobMatches', '' ),
    # ( 'NumJobReconnects', '' ),
    # ( 'NumJobStarts', '' ),
    # ( 'NumPids', '' ),
    # ( 'NumRestarts', '' ),
    # ( 'NumShadowExceptions', '' ),
    # ( 'NumShadowStarts', '' ),
    # ( 'NumSystemHolds', '' ),
    # ( 'OtherJobRemoveRequirements', '' ),
      ( 'Out', 'output' ),
    # ( 'Owner', '' ),
    # ( 'ParallelShutdownPolicy', '' ),
    # ( 'PreserveRelativeExecutable', '' ),
    # ( 'ProcId', '' ),
    # ( 'ProportionalSetSizeKb', '' ),
    # ( 'QDate', '' ),
    # ( 'ReleaseReason', '' ),
    # ( 'RemoteIwd', '' ),
    # ( 'RemotePool', '' ),
    # ( 'RemoteSysCpu', '' ),
    # ( 'RemoteUserCpu', '' ),
    # ( 'RemoteWallClockTime', '' ),
    # ( 'RemoveKillSig', '' ),
    # ( 'RequestCpus', '' ),
    # ( 'RequestDisk', '' ),
    # ( 'RequestedChroot', '' ),
      ( 'RequestMemory', 'request_memory' ),
      ( 'Requirements', 'requirements' ),
    # ( 'ResidentSetSize', '' ),
      ( 'ShouldTransferFiles', 'should_transfer_files' ),
    # ( 'StackSize', '' ),
    # ( 'StageOutFinish', '' ),
    # ( 'StageOutStart', '' ),
    # ( 'StreamErr', '' ),
    # ( 'StreamOut', '' ),
    # ( 'SubmitterAutoregroup', '' ),
    # ( 'SubmitterGroup', '' ),
    # ( 'SubmitterNegotiatingGroup', '' ),
    # ( 'TotalSuspensions', '' ),
    # ( 'TransferErr', '' ),
      ( 'TransferExecutable', 'transfer_executable' ),
    # ( 'TransferIn', '' ),
    # ( 'TransferInputSizeMB', '' ),
    # ( 'TransferOut', '' ),
      ( 'TransferOutput', 'transfer_output_files' ),
    # ( 'TransferringInput', '' ),
    # ( 'TransferringOutput', '' ),
    # ( 'TransferQueued', '' ),
      ( 'UserLog', 'log' ),
    # ( 'WantGracefulRemoval', '' ),
    # ( 'WindowsBuildNumber', '' ),
    # ( 'WindowsMajorVersion', '' ),
    # ( 'WindowsMinorVersion', '' ),
    # ( 'X509UserProxy', '' ),
    # ( 'X509UserProxyEmail', '' ),
    # ( 'X509UserProxyExpiration', '' ),
    # ( 'X509UserProxyFirstFQAN', '' ),
    # ( 'X509UserProxyFQAN', '' ),
    # ( 'X509UserProxySubject', '' ),
    # ( 'X509UserProxyVOName', ''  )
    ]

VALUE_TRANSLATION_LIST = [
      ( 1, 'standard' ),
      ( 5, 'vanilla' ),
      ( 7, 'scheduler' ),
      ( 8, 'MPI' ),
      ( 9, 'grid' ),
      ( 10, 'java' ),
      ( 11, 'parallel' ),
      ( 12, 'local' ),
      ( 13, 'vm' ),
      ]


def makeTwoWayMapping(list):
    dictAB = dict()
    dictBA = dict()
    for a, b in list:
        dictAB[a] = b
        dictBA[b] = a
        
    return dictAB, dictBA

adAttrs, jobAttrs = makeTwoWayMapping(ATTR_TRANSLATION_LIST)
adValues, jobValues = makeTwoWayMapping(VALUE_TRANSLATION_LIST)

def toAd(attr,value=None):
    '''
    
    '''
    
    adAttr = jobAttrs.get(attr)
    if not adAttr:
        adAttr = transformToAd(attr)
    
    newValue = jobValues.get(value,value)
    
    return adAttr,newValue

def toJob(adAttr, value=None):
    '''
    
    '''
    
    attr = adAttrs.get(adAttr)
    if not attr:
        attr = transformToJob(adAttr)
    
    newValue = adValues.get(value)
    if not newValue:
        newValue = transformValueToJob(attr, value)
    
    return attr,newValue 


def transformToJob(adAttr):
    '''
    '''
    
    words = [word.lower() for word in re.split('([A-Z][a-z\d]*)', adAttr) if word]
    attr = '_'.join(words)
    return attr

def transformToAd(attr):
    '''
    '''
    
    adAttr = ''.join([word.capitalize() for word in attr.split('_')])
    return adAttr

def transformValueToJob(attr, value):
    '''
    '''
    if(attr == 'requirements'):
        return re.sub('\(|\)','',value)
    else:
        return value

##########################################
#
#    Unit Tests
#
##########################################

def runTests():    
    print('testing')
    assert toAd('exe')[0] == 'Exe'
    assert toJob('Cmd')[0] == 'executable'
    assert toJob('ShouldTransferFiles')[0] == 'should_transfer_files'
    assert toJob('JobUniverse',5) == ('universe','vanilla') 
    assert toAd('initialdir')[0] == 'Iwd'
    assert toJob('Iwd')[0] == 'initialdir'
    assert transformValueToJob('requirements', '( Arch == \'X86_64\' && OpSys == \'WINDOWS\' )') == ' Arch == \'X86_64\' && OpSys == \'WINDOWS\' '
    assert transformValueToJob('requirements', "( Arch == 'X86_64' && OpSys == 'WINDOWS' ) && ( TARGET.Disk >= RequestDisk ) && ( TARGET.Memory >= RequestMemory ) && ( TARGET.HasFileTransfer )") == " Arch == 'X86_64' && OpSys == 'WINDOWS'  &&  TARGET.Disk >= RequestDisk  &&  TARGET.Memory >= RequestMemory  &&  TARGET.HasFileTransfer "
      
    print('passed')
    
if __name__ == '__main__':
    runTests()
