INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/util/randomValues.inc;

hydra.HostPrms-names          = host1    host2   host3;

hydra.VmPrms-names            = vm1      vm2     vm3;
hydra.VmPrms-hostNames        = host1    host2   host3;

hydra.GemFirePrms-names       = gemfire1 gemfire2 gemfire3;
hydra.GemFirePrms-hostNames   = host1    host2    host3;

hydra.ClientPrms-names        = positiveClient gemfire1Client negativeClient  gemfire2Client otherClient1 otherClient2;
hydra.ClientPrms-vmNames      = vm1            vm1            vm2             vm2            vm3          vm3;
hydra.ClientPrms-gemfireNames = gemfire1       gemfire1       gemfire2        gemfire2       gemfire3     gemfire3;
hydra.ClientPrms-vmQuantities = 1              1              1               1              1            1;
hydra.ClientPrms-vmThreads    = 2              2              2               2              4            4;

// threads that do the updates
THREADGROUP positiveUpdateThread totalThreads = 1  totalVMs = 1  clientNames = positiveClient;
THREADGROUP negativeUpdateThread totalThreads = 1  totalVMs = 1  clientNames = negativeClient;

// threads in the same VMs as the positiveUpdateThread and negativeUpdateThread
THREADGROUP otherPositiveUpdateThreads totalThreads = 1  totalVMs = 1 clientNames = positiveClient;
THREADGROUP otherNegativeUpdateThreads totalThreads = 1  totalVMs = 1  clientNames = negativeClient;

// threadgroups that contain all threads in 1 VM
THREADGROUP gemfire1ClientThreads totalThreads = 2  totalVMs = 1  clientNames = gemfire1Client;
THREADGROUP gemfire2ClientThreads totalThreads = 2  totalVMs = 1  clientNames = gemfire2Client;
THREADGROUP otherClientVM1Threads totalThreads = 4 totalVMs = 1 clientNames = otherClient1;
THREADGROUP otherClientVM2Threads totalThreads = 4 totalVMs = 1 clientNames = otherClient2;

STARTTASK    taskClass   = mirror.MirrorBB taskMethod  = HydraTask_initialize
             clientNames = positiveClient;

STARTTASK    taskClass   = util.CacheBB taskMethod  = HydraTask_initialize
             clientNames = positiveClient;

STARTTASK    taskClass   = util.RegionDefinition taskMethod = HydraTask_initialize;

INITTASK     taskClass   = mirror.MirrorTest taskMethod  = HydraTask_initWithListener
             threadGroups = positiveUpdateThread 
             util.CachePrms-dataPolicyAttribute = normal 
             mirror.MirrorPrms-listenerClassName = mirror.EventListener1;

INITTASK     taskClass   = mirror.MirrorTest taskMethod  = HydraTask_initWithListener
             threadGroups = negativeUpdateThread
             util.CachePrms-dataPolicyAttribute = normal
             mirror.MirrorPrms-listenerClassName = mirror.EventListener2;

INITTASK     taskClass   = mirror.MirrorTest taskMethod  = HydraTask_initWithListener
             threadGroups = gemfire1ClientThreads
             util.CachePrms-dataPolicyAttribute = replicate
             mirror.MirrorPrms-listenerClassName = mirror.EventListener3;

INITTASK     taskClass   = mirror.MirrorTest taskMethod  = HydraTask_initWithListener
             threadGroups = gemfire2ClientThreads
             util.CachePrms-dataPolicyAttribute = replicate
             mirror.MirrorPrms-listenerClassName = mirror.EventListener4;

INITTASK     taskClass   = mirror.MirrorTest taskMethod  = HydraTask_initWithListener
             threadGroups = otherClientVM1Threads
             util.CachePrms-dataPolicyAttribute = replicate
             mirror.MirrorPrms-listenerClassName = mirror.EventListener5;

INITTASK     taskClass   = mirror.MirrorTest taskMethod  = HydraTask_initWithListener
             threadGroups = otherClientVM2Threads
             util.CachePrms-dataPolicyAttribute = replicate
             mirror.MirrorPrms-listenerClassName = mirror.EventListener6;

TASK         taskClass   = mirror.MirrorTest taskMethod  = HydraTask_updatePositive
             threadGroups = positiveUpdateThread;

TASK         taskClass   = mirror.MirrorTest taskMethod  = HydraTask_updateNegative
             threadGroups = negativeUpdateThread;

TASK         taskClass   = mirror.MirrorTest taskMethod  = HydraTask_readPositive
             threadGroups = otherPositiveUpdateThreads;

TASK         taskClass   = mirror.MirrorTest taskMethod  = HydraTask_readNegative
             threadGroups = otherNegativeUpdateThreads;

TASK         taskClass   = mirror.MirrorTest taskMethod  = HydraTask_readKeys
             threadGroups = gemfire1ClientThreads, gemfire2ClientThreads, 
                            otherClientVM1Threads, otherClientVM2Threads;

ENDTASK      taskClass   = diskReg.DiskRegTest taskMethod  = HydraTask_mirrorKeysEndTask;

hydra.GemFirePrms-stopSystemsAfterTest = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.Prms-totalTaskTimeSec = 900;
hydra.Prms-maxResultWaitSec = 600;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-serialExecution = false;
hydra.Prms-alwaysDoEndTasks = false;
hydra.Prms-maxClientShutdownWaitSec = 360;
hydra.Prms-finalClientSleepSec = 30;
hydra.Prms-maxEndTaskResultWaitSec = 1800;

// Logging params
hydra.log.LogPrms-file_logging = true;

util.TestHelperPrms-printElementsLimit = 0;
util.TestHelperPrms-printStringLimit = 100;
util.TestHelperPrms-printObjectDepth = 0;
util.TestHelperPrms-minTaskGranularitySec = 30;

util.ValueHolderPrms-useExtraObject = true;
util.CachePrms-keyValueWaitTime = 300;

// do not specify dataPolicy here; it is specified in the task attributes above
util.RegionDefPrms-regionSpecs = "
   specName = region1:
       scope = noAck ack global: 
   ";
util.RegionDefPrms-VMRegionSpecName = region1;
util.RegionDefPrms-regionDefUsage = useOneRegionSpec;

mirror.MirrorPrms-totalNumObjectsToMirror = 40000;
hydra.VmPrms-extraVMArgs = "-Xmx600m";
