INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p_4_locator.inc;

THREADGROUP locatorThreads
    totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
                                       * ${locatorThreadsPerVM}
                   ncf     
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"locator\", ${locatorHosts}, true)"
                   ncf;    
THREADGROUP accessorThreads
    totalThreads = fcn ${${A}Hosts} * ${${A}VMsPerHost}
                                    * ${${A}ThreadsPerVM}
                   ncf     
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${A}\", ${${A}Hosts}, true)"
                   ncf;
THREADGROUP dataStoreThreads
    totalThreads = fcn ${${B}Hosts} * ${${B}VMsPerHost}
                                    * ${${B}ThreadsPerVM}
                   ncf     
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${B}\", ${${B}Hosts}, true)"
                   ncf;
THREADGROUP adminThreads
    totalThreads = fcn ${${C}Hosts} * ${${C}VMsPerHost}
                                    * ${${C}ThreadsPerVM}
                   ncf     
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${C}\", ${${C}Hosts}, true)"
                   ncf;
THREADGROUP cliThreads
    totalThreads = fcn ${${D}Hosts} * ${${D}VMsPerHost}
                                    * ${${D}ThreadsPerVM}
                   ncf     
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${D}\", ${${D}Hosts}, true)"
                   ncf;

// init stop/start
INITTASK taskClass    = util.StopStartVMs  taskMethod = StopStart_initTask
         threadGroups = dataStoreThreads, accessorThreads;

// init locators
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_createLocator
         runMode      = always
         threadGroups = locatorThreads;
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_startLocatorAndDS
         runMode      = always
         threadGroups = locatorThreads;

// init members
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_configurableInit
         runMode      = always
         management.test.cli.CommandPrms-numMembersJoinDSOnly=${numMembersJoinDSOnly}
         management.test.cli.CommandPrms-numMembersCreateCacheOnly=${numMembersCreateCacheOnly}
         management.test.cli.CommandPrms-numMembersToHostRegion=${numDataStoreMembersToHostRegion}
         threadGroups = dataStoreThreads;
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_configurableInit
         management.test.cli.CommandPrms-createProxyRegions=true 
         runMode      = always
         threadGroups = accessorThreads;
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_writeMemberDescriptionToBB
         threadGroups = accessorThreads, dataStoreThreads;
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_verifyRegionMBeans
         threadGroups = accessorThreads, dataStoreThreads, locatorThreads;

INITTASK taskClass = util.AdminHelper  taskMethod = HydraTask_initializeAdminDS
         threadGroups = adminThreads;

INITTASK taskClass = management.test.cli.CommandTest taskMethod = HydraTask_writeJmxManagerSettingToBB
         threadGroups = accessorThreads, dataStoreThreads, cliThreads;

// check managers
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_verifyNoManagers
         threadGroups = accessorThreads, dataStoreThreads, locatorThreads;

// initialize the jvms to run the CLI
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_logBBs;
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_initAndConnectCLI
         threadGroups = cliThreads;

// check managers
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_verifyAutoManagers
         threadGroups = accessorThreads, dataStoreThreads, locatorThreads;
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_verifyAutoNumManagers
         threadGroups = accessorThreads, dataStoreThreads, locatorThreads;

INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_verifyMemberMBeans
         threadGroups = accessorThreads, dataStoreThreads, locatorThreads;
INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_verifyRegionMBeans
         threadGroups = accessorThreads, dataStoreThreads, locatorThreads;

INITTASK taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_writeRegionNamesToBB
         threadGroups = dataStoreThreads, accessorThreads;

// load data into regions
// todo
//INITTASK taskClass = management.test.cli.CommandTest taskMethod = HydraTask_load
//         batch
//         management.test.cli.CommandPrms-numToLoadEachRegion=50
//         threadGroups = dataStoreThreads, accessorThreads;

// todo
//INITTASK taskClass    = management.test.cli.CommandTest taskMethod   = HydraTask_addIndex
//         threadGroups = dataStoreThreads, accessorThreads;

// todo
//INITTASK taskClass    = management.test.cli.CommandTest taskMethod   = HydraTask_addFunction
//         threadGroups = dataStoreThreads, accessorThreads;

// todo
//TASK     taskClass     = management.test.cli.Operations taskMethod = HydraTask_doOperations
//         threadGroups  = dataStoreThreads, accessorThreads;

// todo
//TASK     taskClass     = management.test.cli.CommandTest taskMethod = HydraTask_execCommands
//         threadGroups = cliThreads;

CLOSETASK  taskClass    = management.test.cli.CommandTest taskMethod = HydraTask_checkForCommandErrors;

ENDTASK  taskClass    = util.LogChecker  taskMethod = HydraTask_checkLogs;

hydra.VmPrms-extraClassPaths += $GEMFIRE/lib/gfsh-dependencies.jar;

hydra.Prms-totalTaskTimeSec = 120;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;
hydra.Prms-maxResultWaitSec = 900;

INCLUDE $JTESTS/management/test/cli/regions.inc;
hydra.RegionPrms-poolName           = none; // override pool settings in regions.inc
management.test.cli.CommandPrms-createPersistentRegions = true;

hydra.VmPrms-extraClassPaths += "$GEMFIRE/lib/antlr.jar";

util.ValueHolderPrms-useExtraObject=false;

// gfsh connection/manager settings
hydra.GemFirePrms-jmxManagerPort=true; // required to have hydra autogenerate a port

util.AdminHelperPrms-alertListener = util.AlertLogListener;

util.OperationsClientPrms-entryOperations = ONEOF add add getNew getNew update update update invalidate get destroy replace remove FOENO;
util.OperationsClientPrms-upperThreshold = 100;
util.OperationsClientPrms-upperThresholdOperations = ONEOF destroy remove FOENO;
util.OperationsClientPrms-lowerThreshold = 50;
util.OperationsClientPrms-lowerThresholdOperations = add ;
util.TestHelperPrms-minTaskGranularitySec = 10; // length of ops per region

management.test.cli.CommandPrms-connectToLocator = true;
management.test.cli.CommandPrms-functionsRegistered = true;
management.test.cli.CommandPrms-indexAdded = true;

hydra.GemFirePrms-offHeapMemorySize = 10m;
hydra.RegionPrms-enableOffHeapMemory = ONEOF true false FOENO;
