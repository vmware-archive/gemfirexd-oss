hydra.Prms-testRequirement = "Concurrent test to verify delta gii scenarios described in functional spec Section 5.2";
hydra.Prms-testDescription = "TBD (from function or test spec)";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_2.inc;

// all topology groups are peers
hydra.GemFirePrms-distributedSystem = ds;

hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xmx100m\", ${${A}Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xmx100m\", ${${B}Hosts}, true)"
                             ncf;

// reduce the tombstone expiration timeout (default is 10 minutes), limit the number of tombstones queued up for garbage collection
hydra.VmPrms-extraVMArgs  += "-Dgemfire.tombstone-gc-threshold=5000 -Dgemfire.tombstone-timeout=300000";

THREADGROUP providerThreads
    totalThreads = fcn
                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${A}\", ${${A}Hosts}, true)"
                   ncf;
THREADGROUP requesterThreads 
    totalThreads = fcn
                   ${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${B}\", ${${B}Hosts}, true)"
                   ncf;

// initTasks with runMode always
INITTASK     taskClass   = deltagii.DeltaGIITest  taskMethod = HydraTask_initialize
             threadGroups = providerThreads, requesterThreads
             runMode = always;

INITTASK     taskClass   = deltagii.DeltaGIITest  taskMethod = HydraTask_load
             threadGroups = providerThreads, requesterThreads
             runMode = once;

// reset unique key index (bring NameFactory.POSITIVE_NAME_COUNTER past existing keys)
INITTASK     taskClass   = deltagii.DeltaGIITest  taskMethod = HydraTask_resetUniqueKeyIndex
             threadGroups = providerThreads, requesterThreads
             runMode = once;

// initTasks with runMode dynamic
INITTASK     taskClass   = deltagii.DeltaGIITest  taskMethod = HydraTask_verifyFromSnapshotAndSync
             threadGroups = requesterThreads
             runMode = dynamic;

INITTASK     taskClass   = deltagii.DeltaGIITest  taskMethod = HydraTask_verifyDeltaGII
             threadGroups = requesterThreads
             runMode = dynamic;
  
TASK         taskClass   = deltagii.DeltaGIITest  taskMethod = HydraTask_giiProvider
             threadGroups = providerThreads;
             
TASK         taskClass   = deltagii.DeltaGIITest  taskMethod = HydraTask_giiRequester
             threadGroups = requesterThreads;

hydra.Prms-totalTaskTimeSec = 31536000;  // test terminated by numExecutions
deltagii.DeltaGIIPrms-numExecutions = 3;
hydra.Prms-maxResultWaitSec = 600;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = RANGE 1 50 EGNAR;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = true;

hydra.ConfigPrms-cacheConfig = cache1;
hydra.ConfigPrms-regionConfig = persistRegion;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.Prms-useFixedRandomInMaster= true;
hydra.RegionPrms-names           = persistRegion;
hydra.RegionPrms-regionName      = persistRegion;
hydra.RegionPrms-dataPolicy      = persistentReplicate;
hydra.RegionPrms-diskStoreName   = diskStore1;
hydra.RegionPrms-diskSynchronous = true;
hydra.RegionPrms-cacheListeners = util.SummaryLogListener;

hydra.DiskStorePrms-names = diskStore1;
hydra.DiskStorePrms-allowForceCompaction = true;  // this test explicitly calls compaction
hydra.DiskStorePrms-autoCompact = false;  // this test explicitly calls compaction
hydra.DiskStorePrms-maxOplogSize = 1; // frequent new logs

deltagii.DeltaGIIPrms-useUniqueKeys = true;

deltagii.DeltaGIIPrms-operations = ONEOF add add destroy destroy invalidate invalidate update update get get getNew getNew putAll FOENO;
deltagii.DeltaGIIPrms-lowerThreshold = 10;
deltagii.DeltaGIIPrms-upperThreshold = 100;
deltagii.DeltaGIIPrms-lowerThresholdOperations = ONEOF add putAll FOENO;
deltagii.DeltaGIIPrms-upperThresholdOperations = ONEOF destroy FOENO;
deltagii.DeltaGIIPrms-numPutAllNewKeys = RANGE 1 10 EGNAR;
deltagii.DeltaGIIPrms-numPutAllExistingKeys = RANGE 1 10 EGNAR;
deltagii.DeltaGIIPrms-numEntriesToLoad = 100;

deltagii.DeltaGIIPrms-providerDoesOps = false;
deltagii.DeltaGIIPrms-providerOperations = get;

deltagii.DeltaGIIPrms-expectDeltaGII = true;

util.TestHelperPrms-minTaskGranularitySec = 30;
util.StopStartPrms-stopModes = ONEOF nice_exit nice_kill mean_exit mean_kill FOENO;
