hydra.Prms-testRequirement = "Test pre-6.5 disk file conversion to 6.5 disk files";
hydra.Prms-testDescription = "
   The test creates 2 distributed systems: one pre-6.5 and the other 6.5 or later (but < 7.0)
   In the pre-6.5 distributed system:
      1) create regions
      2) do random ops to create data
      3) write snapshot to BB
      4) disconnect from distributed system
   Convert pre-6.5 disk files
   In the 6.5 or later distributed system
      1) initialize with converted disk files
      2) compare to bb
      3) do random ops with validation
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_4.inc;

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
                   (${${D}Hosts} * ${${D}VMsPerHost} * ${${D}ThreadsPerVM}) 
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${D}\", ${${D}Hosts}, true)"
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
                                ncf;
hydra.VersionPrms-names   = oldVersion     newVersion;
hydra.VersionPrms-version = ${oldVersion}  ${newVersion};
hydra.VersionPrms-gemfireHome =
  fcn "hydra.VersionDescription.getGemFireHome(\"${oldVersion}\")" ncf
  fcn "hydra.VersionDescription.getGemFireHome(\"${newVersionHome}\")" ncf
  ;


// ************************************************************************************
// Initialization tasks

// initialize for stop/start
INITTASK     taskClass     = util.StopStartVMs  taskMethod = StopStart_initTask
             threadGroups = oldVersionThreads, newVersionThreads;

// start the locators for the old and new versions
INITTASK     taskClass   = hydra.DistributedSystemHelper    taskMethod = createLocator
             threadGroups = oldVersionLocatorThread, newVersionLocatorThread;
INITTASK     taskClass   = hydra.DistributedSystemHelper    taskMethod = startLocatorAndDS
             threadGroups = oldVersionLocatorThread, newVersionLocatorThread;

// initialize the oldVersion jvms 
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegions
             threadGroups = oldVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createExtraRegions
             threadGroups = oldVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createXmlFile
             threadGroups = oldVersionThreads;

// prepare for disk file conversion in the newVersion jvms 
// this creates new version xml files to be used by the conversion tool
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegions
             threadGroups = newVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createXmlFile
             threadGroups = newVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_disconnect
             threadGroups = newVersionThreads;
INITTASK     taskClass   = diskRecovery.RecoveryTestVersionHelper  taskMethod = removeDiskFiles
             threadGroups = newVersionThreads;

// ************************************************************************************
// oldVersion tasks

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_doOperations
             threadGroups = oldVersionThreads;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_writeToBB
             threadGroups = oldVersionThreads;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_disconnect
             threadGroups = oldVersionThreads;

// ************************************************************************************
// newVersion tasks

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_doSingleConvert
             threadGroups = newVersionThreads;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegionsConvertedFirst
             threadGroups = newVersionThreads;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_verifyFromSnapshot
             threadGroups = newVersionThreads;
  
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_disconnect
             threadGroups = newVersionThreads;

INITTASK     taskClass   = util.PersistenceUtil  taskMethod = doOfflineValAndCompaction
             threadGroups = newVersionLocatorThread;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegions
             threadGroups = newVersionThreads;

INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_verifyFromSnapshot
             threadGroups = newVersionThreads;
  
INITTASK     taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_prepareForNewVersionOps
             threadGroups = newVersionThreads;
  
// ************************************************************************************
// newVersion test tasks

TASK         taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_doOperations
             threadGroups = newVersionThreads;

// hits bug 42633 when converting to 65 versions, which keeps us from getting coverage
//TASK         taskClass   = util.PersistenceUtil  taskMethod = doOnlineBackup
//             threadGroups = newVersionThreads
//             maxTimesToRun = 1
//             maxThreads = 1;

// ************************************************************************************
// newVersion close tasks

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_writeToBB
             threadGroups = newVersionThreads;

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_disconnect
             threadGroups = newVersionThreads;

CLOSETASK    taskClass   = util.PersistenceUtil  taskMethod = HydraTask_initialize
             threadGroups = newVersionLocatorThread;

CLOSETASK    taskClass   = util.PersistenceUtil  taskMethod = doOfflineValAndCompaction
             threadGroups = newVersionLocatorThread;

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_createRegions
             threadGroups = newVersionThreads;

CLOSETASK    taskClass   = diskRecovery.RecoveryTest  taskMethod = HydraTask_verifyFromSnapshot
             threadGroups = newVersionThreads;
  
hydra.Prms-totalTaskTimeSec = 180;
hydra.Prms-maxResultWaitSec = 1200;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = RANGE 1 50 EGNAR;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

// ************************************************************************************
hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

// ************************************************************************************

diskRecovery.RecoveryPrms-operations = ONEOF add add destroy destroy invalidate invalidate update update get get getNew getNew 
                                             putAll putAll cacheOperations cacheOperations FOENO;
diskRecovery.RecoveryPrms-lowerThreshold = 10;
diskRecovery.RecoveryPrms-upperThreshold = 100;
diskRecovery.RecoveryPrms-lowerThresholdOperations = ONEOF add putAll add putAll cacheOperations FOENO;
diskRecovery.RecoveryPrms-upperThresholdOperations = ONEOF destroy cacheOperations FOENO;
diskRecovery.RecoveryPrms-numPutAllNewKeys = RANGE 1 10 EGNAR;
diskRecovery.RecoveryPrms-numPutAllExistingKeys = RANGE 1 10 EGNAR;
diskRecovery.RecoveryPrms-useUniqueKeys = true;
diskRecovery.RecoveryPrms-convertWithNewVersionXml = ${convertWithNewVersionXml};
diskRecovery.RecoveryPrms-valueClassName = ${valueClassName};
diskRecovery.RecoveryPrms-registerSerializer = ${registerSerializer};

util.TestHelperPrms-minTaskGranularitySec = 180;
