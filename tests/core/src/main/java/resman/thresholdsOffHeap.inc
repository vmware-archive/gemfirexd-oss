INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p_2.inc;


THREADGROUP accessorThreads
    totalThreads = fcn
                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${A}\", ${${A}Hosts}, true)"
                   ncf;

THREADGROUP dataStoreThreads
    totalThreads = fcn
                   ${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${B}\", ${${B}Hosts}, true)"
                   ncf;

INITTASK         taskClass   = resman.ThresholdsTest   taskMethod = HydraTask_HA_initializeAccessor
                 runMode = once
                 threadGroups = accessorThreads;

INITTASK         taskClass   = resman.ThresholdsTest   taskMethod = HydraTask_HA_initializeDataStore
                 runMode = once
                 threadGroups = dataStoreThreads;

CLOSETASK        taskClass   = parReg.ParRegUtil  taskMethod = HydraTask_rebalance
                 threadGroups = accessorThreads;

CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = closeAllRegions;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;

ENDTASK taskClass = util.LogChecker taskMethod = HydraTask_checkLogs;

hydra.GemFirePrms-stopSystemsAfterTest = true;
hydra.GemFirePrms-offHeapMemorySize=${offHeapMB};

// parReg.ParRegPrms-secondsToRun determines the running length rather than 
// totalTaskTimeSec because of the test's pausing scheme; without this the
// test could hang 
hydra.Prms-totalTaskTimeSec = 31536000;  
hydra.Prms-maxResultWaitSec = 2400;
hydra.Prms-serialExecution = false;
hydra.Prms-clientShutdownHook = parReg.ParRegUtil dumpAllPartitionedRegions;
hydra.CachePrms-resourceManagerName = manager;

hydra.ResourceManagerPrms-names = manager;
hydra.ResourceManagerPrms-evictionOffHeapPercentage = ${evictionThreshold};
hydra.ResourceManagerPrms-criticalOffHeapPercentage = ${criticalThreshold};

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = ONEOF 10 15 FOENO;

util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;


hydra.RegionPrms-names          = accessorRegion           dataStoreRegion;
hydra.RegionPrms-regionName     = partitionedRegion;
hydra.RegionPrms-cacheLoader    = parReg.ParRegLoader;
hydra.RegionPrms-dataPolicy     = partition;
hydra.RegionPrms-partitionName  = accessorPR               dataStorePR;
hydra.RegionPrms-enableOffHeapMemory = true;

hydra.PartitionPrms-names           = accessorPR          dataStorePR;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-localMaxMemory  = 0                   default;


hydra.RegionPrms-evictionAttributes	= lruHeapPercentage default overflowToDisk;
hydra.RegionPrms-diskStoreName  = disk;
hydra.DiskStorePrms-names       = disk;
hydra.RegionPrms-diskSynchronous = true;
hydra.DiskStorePrms-maxOplogSize = 512;
hydra.DiskStorePrms-autoCompact = true;


util.CachePrms-useDeclarativeXmlFile = ONEOF true false FOENO;

util.TestHelperPrms-minTaskGranularitySec = ONEOF 15 20 25 30 FOENO;

// operations will be chosen from thresholdOperations, otherwise the operations
// will be chosen from entryOperations
parReg.ParRegPrms-entryOperations = ONEOF add add add getNew getNew update get FOENO; 
parReg.ParRegPrms-secondsToRun = 300;

hydra.VmPrms-extraVMArgsSUN = "-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:CMSInitiatingOccupancyFraction=30";
hydra.VmPrms-extraVMArgsJRockit = "-Xgc:gencon -XXgcTrigger:98 -XXfullCompaction -Xns:10m";
hydra.VmPrms-extraVMArgsIBM = "-Xgcpolicy:gencon -verbose:sizes";
hydra.VmPrms-extraVMArgs += "-Xmx${heapMB}m -Xms${heapMB}m -Dgemfire.disk.recoverValuesSync=true";

hydra.log.LogPrms-mergeLogFiles = onFailure;

resman.ResourceManPrms-offHeapVerifyTargetCount = ${accessorHosts};
