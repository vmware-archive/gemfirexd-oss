INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_2.inc;

// all topology groups are peers
hydra.GemFirePrms-distributedSystem = ds;

THREADGROUP controllerThread
    totalThreads = 1
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${A}\", ${${A}Hosts}, true)"
                   ncf;
THREADGROUP peerThreads
    totalThreads = fcn
                   ${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${B}\", ${${B}Hosts}, true)"
                   ncf;

INITTASK taskClass    = diskRecovery.RecoveryTest
         taskMethod   = HydraTask_initialize
         threadGroups = peerThreads
         runMode      = always
         diskRecovery.RecoveryPrms-regionConfigNames = persistRegion
         ;
INITTASK taskClass    = diskRecovery.RecoveryTest
         taskMethod   = HydraTask_recordStartupState
         threadGroups = peerThreads
         runMode      = dynamic
         ;
// the controller thread is also an admin vm
INITTASK taskClass    = util.AdminHelper
         taskMethod   = HydraTask_initializeAdminDS
         threadGroups = controllerThread
         ;
INITTASK taskClass    = diskRecovery.RecoveryTest
         taskMethod   = HydraTask_load
         threadGroups = peerThreads
         batch
         ;

INITTASK taskClass = memscale.OffHeapHelper taskMethod = waitForOffHeapSilence;
INITTASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;

INITTASK taskClass    = diskRecovery.RecoveryTest
         taskMethod   = HydraTask_logRegions
         threadGroups = peerThreads
         ;
INITTASK taskClass    = diskRecovery.RecoveryTest
         taskMethod   = HydraTask_verifyDeltaGII
         threadGroups = peerThreads
         runMode      = dynamic
         ;

TASK taskClass     = diskRecovery.RecoveryTest
     taskMethod    = HydraTask_latestRecoveryTest
     threadGroups  = controllerThread
     maxTimesToRun = 1
     ;

CLOSETASK taskClass    = diskRecovery.RecoveryTest
          taskMethod   = HydraTask_verifyRegions
          threadGroups = peerThreads
          ;
CLOSETASK taskClass    = diskRecovery.RecoveryTest
          taskMethod   = HydraTask_verifyStartupState
          threadGroups = controllerThread
          ;

RANDOMINCLUDE $JTESTS/memscale/configs/enableOffHeap.conf;
INCLUDE $JTESTS/memscale/configs/verifyOffHeap.conf;

hydra.Prms-totalTaskTimeSec = 1200;
hydra.Prms-maxResultWaitSec = 600;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = RANGE 1 50 EGNAR;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;
hydra.CachePrms-lockLease       = 3600;

hydra.RegionPrms-names           = persistRegion;
hydra.RegionPrms-scope           = ONEOF ack global FOENO;
hydra.RegionPrms-dataPolicy      = persistentReplicate;
hydra.RegionPrms-diskStoreName   = diskStore1;
hydra.RegionPrms-diskSynchronous = true;
//hydra.RegionPrms-cacheListeners = util.SummaryLogListener;

hydra.DiskStorePrms-names = diskStore1;
hydra.DiskStorePrms-autoCompact = true;
hydra.DiskStorePrms-maxOplogSize = 1; // frequent new logs

diskRecovery.RecoveryPrms-numRootRegions           = 5;
diskRecovery.RecoveryPrms-maxNumEntriesPerRegion   = 15000;
diskRecovery.RecoveryPrms-concurrentRegionCreation = ${concurrentRegionCreation};
diskRecovery.RecoveryPrms-persistenceObserver      = diskRecovery.RecoveryTestObserver;

util.TestHelperPrms-minTaskGranularitySec = 60;
util.StopStartPrms-stopModes = ${stopMode};
