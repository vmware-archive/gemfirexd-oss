hydra.Prms-testRequirement = "Concurrent test to verify delta gii scenarios described in functional spec Section 5.2";
hydra.Prms-testDescription = "One member acts as an image source (the provider) and other VMs as requestors.  Various scenarios (described in the functional spec) use this include for the basic test format.
1 - the requester missing NO operations while it is offline (verify delta gii)
2 - the requester misses creates and updates (no destroys) while it is offline (verify delta gii)
3 - the requester a misses destroys occuring in the provider while it is offline (verify full gii)
4 - the requester misses ops (including destroys) in the provider (but no tombstone gc) while it is offline (verify delta gii)
5 - the requester misses a region clear in the provider while it is offline (verify full gii)
6 - expect a full gii in new members (which join while other members are doing a delta gii)
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_2.inc;

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

// record vms with StopStartVMs util
INITTASK taskClass    = util.StopStartVMs
         taskMethod   = StopStart_initTask
         threadGroups = providerThreads, requesterThreads
         runMode      = once;

INITTASK taskClass    = deltagii.DeltaGIITest
         taskMethod   = HydraTask_initialize
         threadGroups = providerThreads, requesterThreads
         runMode      = always;

INITTASK taskClass    = deltagii.DeltaGIITest
         taskMethod   = HydraTask_load
         threadGroups = providerThreads, requesterThreads
         runMode      = once;

// reset unique key index (bring NameFactory.POSITIVE_NAME_COUNTER past existing keys)
INITTASK taskClass    = deltagii.DeltaGIITest
         taskMethod   = HydraTask_resetUniqueKeyIndex
         threadGroups = providerThreads, requesterThreads
         runMode      = once;

INITTASK taskClass    = deltagii.DeltaGIITest
         taskMethod   = HydraTask_verifyFromSnapshotAndSync
         threadGroups = requesterThreads
         runMode      = dynamic;

INITTASK taskClass    = deltagii.DeltaGIITest
         taskMethod   = HydraTask_verifyDeltaGII
         threadGroups = requesterThreads
         runMode      = dynamic;
  
TASK taskClass    = deltagii.DeltaGIITest
     taskMethod   = HydraTask_giiProvider
     threadGroups = providerThreads;
             
TASK taskClass    = deltagii.DeltaGIITest
     taskMethod   = HydraTask_giiRequester
     threadGroups = requesterThreads;

hydra.Prms-totalTaskTimeSec       = 31536000;  // test terminated by numExecutions
hydra.Prms-maxResultWaitSec       = 600;
hydra.Prms-useFixedRandomInMaster = true;

hydra.GemFirePrms-distributedSystem = ds;  // all topology groups are peers
hydra.GemFirePrms-conserveSockets   = true;

hydra.ConfigPrms-cacheConfig  = cache1;
hydra.ConfigPrms-regionConfig = persistRegion;
hydra.CachePrms-names         = cache1;
hydra.CachePrms-searchTimeout = 600;

hydra.RegionPrms-names            = persistRegion;
hydra.RegionPrms-regionName       = persistRegion;
hydra.RegionPrms-dataPolicy       = persistentReplicate;
hydra.RegionPrms-diskStoreName    = diskStore1;
hydra.RegionPrms-diskSynchronous  = true;
hydra.RegionPrms-cacheListeners   = util.SummaryLogListener;

hydra.DiskStorePrms-names                = diskStore1;
hydra.DiskStorePrms-allowForceCompaction = true;  // this test explicitly calls compaction
hydra.DiskStorePrms-autoCompact          = false; // this test explicitly calls compaction
hydra.DiskStorePrms-maxOplogSize         = 1;     // frequent new logs

deltagii.DeltaGIIPrms-expectDeltaGII           = true;
deltagii.DeltaGIIPrms-lowerThreshold           = 10;
deltagii.DeltaGIIPrms-upperThreshold           = 100;
deltagii.DeltaGIIPrms-lowerThresholdOperations = ONEOF add putAll FOENO;
deltagii.DeltaGIIPrms-upperThresholdOperations = ONEOF destroy FOENO;
deltagii.DeltaGIIPrms-numEntriesToLoad         = 100;
deltagii.DeltaGIIPrms-numExecutions            = 3;
deltagii.DeltaGIIPrms-numPutAllExistingKeys    = RANGE 1 10 EGNAR;
deltagii.DeltaGIIPrms-numPutAllNewKeys         = RANGE 1 10 EGNAR;
deltagii.DeltaGIIPrms-operations               = ONEOF add add destroy destroy invalidate invalidate update update get get getNew getNew putAll FOENO;
deltagii.DeltaGIIPrms-providerDoesOps          = false;
deltagii.DeltaGIIPrms-providerOperations       = get;
deltagii.DeltaGIIPrms-useUniqueKeys            = true;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType          = byte[];
util.RandomValuesPrms-elementSize         = RANGE 1 50 EGNAR;
util.ValueHolderPrms-useExtraObject       = true;
util.TestHelperPrms-minTaskGranularitySec = 30;
util.StopStartPrms-stopModes              = ONEOF nice_exit nice_kill mean_exit mean_kill FOENO;

hydra.VmPrms-extraVMArgs = fcn "hydra.TestConfigFcns.duplicate
                                (\"-Xmx100m\", ${${A}Hosts}, true)"
                           ncf
                           ,
                           fcn "hydra.TestConfigFcns.duplicate
                                (\"-Xmx100m\", ${${B}Hosts}, true)"
                           ncf;
