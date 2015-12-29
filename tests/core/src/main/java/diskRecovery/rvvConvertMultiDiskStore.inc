hydra.Prms-testRequirement = "Test pre-7.0 disk file conversion to 7.0 disk files with multiple disk stores";
hydra.Prms-testDescription = "
   The test has 2 distributed systems: one is a pre-7.0 version, and the other is a 7.0 version
   1) create pre-7.0 disk files with random ops in the old version distributed system
   2) pause the old version jvms (one is randomly chosen to write its snapshot to the blackboard)
   3) convert the pre-7.0 disk files to 7.0
   4) startup the new version jvms and validate recovered data
   5) continuing with the new version jvms, run the multiDiskStore test task repeatedly
      until the test is finished
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_5.inc;

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

// make 2 distributed systems; one with old version locator and peers and another
// with new version locator and peers
hydra.GemFirePrms-distributedSystem =
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"oldVersionDS\",    ${oldVersionLocatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"oldVersionDS\",    ${oldVersionHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"newVersionDS\",    ${newVersionLocatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"newVersionDS\",    ${newVersionHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"oldVersionDS\",    ${oldVersionAdminHosts})"
                              ncf;

// define thread groups
// the locator threads are also used as the controller threads
THREADGROUP oldVersionLocatorThread
    totalThreads = fcn
                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${A}\", ${${A}Hosts}, true)"
                   ncf;
THREADGROUP oldVersionThreads
    totalThreads = fcn
                   ${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${B}\", ${${B}Hosts}, true)"
                   ncf;
THREADGROUP newVersionLocatorThread
    totalThreads = fcn
                   ${${C}Hosts} * ${${C}VMsPerHost} * ${${C}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${C}\", ${${C}Hosts}, true)"
                   ncf;
THREADGROUP newVersionThreads 
    totalThreads = fcn
                   ${${D}Hosts} * ${${D}VMsPerHost} * ${${D}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${D}\", ${${D}Hosts}, true)"
                   ncf;

THREADGROUP oldVersionAdminThread
    totalThreads = fcn
                   ${${E}Hosts} * ${${E}VMsPerHost} * ${${E}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${E}\", ${${E}Hosts}, true)"
                   ncf;

// make the old version locator and old version peers use "oldVersion" in VersionPrms-names
// make the new version locator and new version peers use "newVersion" in VersionPrms-names
hydra.ClientPrms-versionNames = fcn "hydra.TestConfigFcns.duplicate
                                     (\"oldVersion\",    ${oldVersionLocatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"oldVersion\",    ${oldVersionHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"newVersion\",    ${newVersionLocatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"newVersion\",    ${newVersionHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"oldVersion\",    ${oldVersionAdminHosts})"
                                ncf;

hydra.VersionPrms-names   = oldVersion     newVersion;
hydra.VersionPrms-version = ${oldVersion}  ${newVersion};
hydra.VersionPrms-gemfireHome =
  fcn "hydra.VersionDescription.getGemFireHome(\"${oldVersion}\")" ncf
  fcn "hydra.VersionDescription.getGemFireHome(\"${newVersionHome}\")" ncf
  ;

// ************************************************************************************
// Init tasks

// initialize for stop/start
INITTASK     taskClass     = util.StopStartVMs  taskMethod = StopStart_initTask
             threadGroups = oldVersionThreads, newVersionThreads;

// start the locators for the old and new versions
INITTASK     taskClass   = diskRecovery.RecoveryTest    taskMethod = HydraTask_createLocator
             threadGroups = oldVersionLocatorThread, newVersionLocatorThread;
INITTASK     taskClass   = diskRecovery.RecoveryTest    taskMethod = HydraTask_startLocatorAndDS
             threadGroups = oldVersionLocatorThread, newVersionLocatorThread;

// oldVersionAdmin will run a backup (with validation and compaction prior to conversion)
INITTASK taskClass = util.AdminHelper  taskMethod = HydraTask_initializeAdminDS
         threadGroups = oldVersionAdminThread;

// initialize the oldVersion jvms (runMode is once)
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_multiRegionInitialize
             threadGroups = oldVersionThreads
             runMode = once;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createExtraRegions
             threadGroups = oldVersionThreads
             runMode = once;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createXmlFile
             threadGroups = oldVersionThreads
             runMode = once;

// initialize the newVersion jvms to create the newVersion disk directories 
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_multiRegionInitialize
             threadGroups = newVersionThreads
             runMode = always;     // Note this is also run on a dynamic restart

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createExtraRegions
             threadGroups = newVersionThreads
             runMode = once;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createXmlFile
             threadGroups = newVersionThreads
             runMode = once;

INITTASK     taskClass   = diskRecovery.RecoveryTestVersionHelper  taskMethod = removeDiskFiles
             threadGroups = newVersionThreads
             runMode = once;

// verify newVersion jvms with runMode dynamic
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_rvvConvertVerifyFromSnapshotAndSync
             threadGroups = newVersionThreads
             runMode = dynamic;
  
// ************************************************************************************
// Regular tasks

TASK         taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_rvvConvertOperationsController
             threadGroups = newVersionLocatorThread;
             
TASK         taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_rvvConvertOperationsResponder
             threadGroups = newVersionThreads, oldVersionThreads, oldVersionAdminThread;

hydra.Prms-totalTaskTimeSec = 31536000;  // test terminated by secondsToRun
diskRecovery.RecoveryPrms-secondsToRun = 1800;
// new backup with validate + compact + validate takes significant time
hydra.Prms-maxResultWaitSec = 3600;      

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = RANGE 1 50 EGNAR;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

//hydra.DiskStorePrms-names = ds1 ds2 .... dsN;
// note the notUsed diskStoreName is used to fool hydra; diskStores are specified programmatically in the test code 
hydra.DiskStorePrms-names = notUsed fcn "hydra.TestConfigFcns.generateNames(\"ds\", \"${numDiskStores}\", false)" ncf;
hydra.DiskStorePrms-autoCompact          = ONEOF true false FOENO;
hydra.DiskStorePrms-maxOplogSize         = RANGE 1 10 EGNAR;
hydra.DiskStorePrms-allowForceCompaction = ONEOF true false FOENO;
hydra.DiskStorePrms-autoCompact          = ONEOF true false FOENO;
hydra.DiskStorePrms-compactionThreshold  = RANGE 0 100 EGNAR;
hydra.DiskStorePrms-diskDirNum           = RANGE 1 10 EGNAR;
hydra.DiskStorePrms-diskDirSizes         = RANGE 25 30 EGNAR;
hydra.DiskStorePrms-maxOplogSize         = RANGE 1 4 EGNAR;
hydra.DiskStorePrms-queueSize            = RANGE 1 1000 EGNAR;
hydra.DiskStorePrms-timeInterval         = RANGE 1 3000 EGNAR;
hydra.DiskStorePrms-writeBufferSize      = RANGE 1000 50000 EGNAR;

diskRecovery.RecoveryPrms-operations = ONEOF add add destroy destroy invalidate invalidate update update get get getNew getNew putAll cacheOperations cacheOperations FOENO;
diskRecovery.RecoveryPrms-lowerThreshold = 10;
diskRecovery.RecoveryPrms-upperThreshold = 200;
diskRecovery.RecoveryPrms-lowerThresholdOperations = ONEOF add putAll cacheOperations FOENO;
diskRecovery.RecoveryPrms-upperThresholdOperations = ONEOF destroy cacheOperations FOENO;
diskRecovery.RecoveryPrms-numPutAllNewKeys = RANGE 1 10 EGNAR;
diskRecovery.RecoveryPrms-numPutAllExistingKeys = RANGE 1 10 EGNAR;

// oldVersion vms cannot enable concurrencyChecks, so we must use uniqueKeys
diskRecovery.RecoveryPrms-useUniqueKeys = true;
diskRecovery.RecoveryPrms-createRegionsWithXml = true;
diskRecovery.RecoveryPrms-convertWithNewVersionXml = ${convertWithNewVersionXml};

util.StopStartPrms-stopModes = ONEOF nice_exit mean_kill mean_exit nice_kill FOENO;

util.TestHelperPrms-minTaskGranularitySec = 30;

