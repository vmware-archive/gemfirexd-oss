INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparamsN.inc;
hydra.ClientPrms-names        = gii1Client gii2Client gii3Client source1 source2;

hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 6 5 5 10 10;

// 3 different VMs will be doing getInitialImage; define a thread for doing getInitialImage
// in each of those 3 VMs, and define a thread in each of the 3 vms to cause a forced
// disconnect, then a threadgroup for the remaining 3 threads in each VM; this takes care 
// of the first 3 VMs
THREADGROUP getInitImageThread1 totalThreads = 1  totalVMs = 1  clientNames = gii1Client;
THREADGROUP threadsInGetInitImageVM1 totalThreads = 4  totalVMs = 1  clientNames = gii1Client;
THREADGROUP extraThread totalThreads = 1  totalVMs = 1  clientNames = gii1Client;

THREADGROUP getInitImageThread2 totalThreads = 1  totalVMs = 1  clientNames = gii2Client;
THREADGROUP threadsInGetInitImageVM2 totalThreads = 4  totalVMs = 1  clientNames = gii2Client;

THREADGROUP getInitImageThread3 totalThreads = 1  totalVMs = 1  clientNames = gii3Client;
THREADGROUP threadsInGetInitImageVM3 totalThreads = 4  totalVMs = 1  clientNames = gii3Client;

// thread to initially load the region; this is the 4th VM
THREADGROUP source1Thread totalThreads = 1 totalVMs = 1 clientNames = source1;
THREADGROUP remainingSource1Threads totalThreads = 9 totalVMs = 1 clientNames = source1;

THREADGROUP source2Thread totalThreads = 1 totalVMs = 1 clientNames = source2;
THREADGROUP remainingSource2Threads totalThreads = 9 totalVMs = 1 clientNames = source2;

STARTTASK    taskClass     = splitBrain.gii.FDInitImageTest taskMethod = StartTask_initialize
             clientNames = source1;

STARTTASK     taskClass     = util.RegionDefinition  taskMethod = HydraTask_initialize;

// initialize the cache now in the VMs that will getInitialImage, rather than later. This 
// is so the threads can get right to the getInitialImage while the other VMs are concurrently
// doing entry operations
INITTASK     taskClass     = splitBrain.gii.FDInitImageTest taskMethod = HydraTask_initCache
             threadGroups  = getInitImageThread1, getInitImageThread2, getInitImageThread3;

INITTASK     taskClass     = splitBrain.gii.FDInitImageTest taskMethod = HydraTask_initRegion
             threadGroups  = source1Thread, source2Thread;

// dataPolicy specified by giiSourceDataPolicy
// numKeys is randomly split between the two source vms (see next task)
INITTASK     taskClass     = splitBrain.gii.FDInitImageTest taskMethod = HydraTask_loadRegion
             threadGroups  = source1Thread, remainingSource1Threads,
                             source2Thread, remainingSource2Threads
             batch;
 
// dataPolicy specified by giiSourceDataPolicy 
// call get for each key, causing values to be pulled from the other source;
// when finished, the region in both vms will have all keys
INITTASK     taskClass     = splitBrain.gii.FDInitImageTest taskMethod = HydraTask_loadRegionWithGets
             threadGroups  = source1Thread, remainingSource1Threads,
                             source2Thread, remainingSource2Threads
             batch;
 
INITTASK     taskClass     = splitBrain.gii.FDInitImageTest taskMethod = HydraTask_verifyRegionSize
             threadGroups  = source1Thread, source2Thread;

INITTASK     taskClass     = splitBrain.gii.FDInitImageTest taskMethod = HydraTask_createFDRegion
             threadGroups = source1Thread, source2Thread, extraThread;

TASK         taskClass     = splitBrain.gii.FDInitImageTest 
             taskMethod    = HydraTask_doGetInitImage
             util.CachePrms-dataPolicyAttribute = REPLICATE
             threadGroups  = getInitImageThread1, getInitImageThread2, getInitImageThread3
             maxTimesToRun = 3;

TASK         taskClass     = splitBrain.gii.FDInitImageTest 
             taskMethod    = HydraTask_blockedGetRegion
             threadGroups  = threadsInGetInitImageVM1, threadsInGetInitImageVM2, threadsInGetInitImageVM3
             maxTimesToRun = 12;

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 1800;
hydra.Prms-maxResultWaitSec = 900;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-serialExecution = false;
hydra.Prms-alwaysDoEndTasks = false;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = 2000;
util.TestHelperPrms-printElementsLimit = 0;
util.TestHelperPrms-printStringLimit = 100;
util.TestHelperPrms-printObjectDepth = 0;

util.ValueHolderPrms-useExtraObject = true;

util.TestHelperPrms-minTaskGranularitySec = 60;

util.CacheDefPrms-cacheSpecs = "
  specName = cache1:
  evictionHeapPercentage = 60:
";

// don't specify dataPolicy here; it is specified above in the task attributes
// and in giiSourceDataPolicy
util.RegionDefPrms-regionSpecs = "
   specName = region1:
       scope = ack noack: 
       eviction = heap:
       evictionAction = overflowToDisk:
       persistBackup = false: 
       isSynchronous = true false: 
       timeInterval = 10:
       bytesThreshold = 0 1 100 1000:
       numDiskDirs = 1 2 3 4:
   specName = FDRegion:
       regionName = FDRegion:
       scope = ack: 
       dataPolicy = normal:
       interestPolicy = all:
       cacheListeners = splitBrain.ForcedDiscListener:
       eviction = lru:
       evictionLimit = 30:
   ";
util.RegionDefPrms-VMRegionSpecName = region1;
util.RegionDefPrms-regionDefUsage = useOneRegionSpec;

hydra.VmPrms-extraVMArgs = "-Xms600m -Xmx600m";

hydra.GemFirePrms-ackSevereAlertThreshold = 10;
hydra.GemFirePrms-enableNetworkPartitionDetection = ${enableNetworkPartitioning};
hydra.GemFirePrms-disableAutoReconnect = ${enableNetworkPartitioning};