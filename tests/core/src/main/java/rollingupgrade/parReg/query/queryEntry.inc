hydra.Prms-testRequirement = "Test in partiontoned region on  query/create/update/destroy/invalidate events";
hydra.Prms-testDescription = "
Test validate entry operations and querying.

The test tests query after event operations on partitioned region.
";
// A Hydra test that performs a number of cache-related operations and 
// querying in a distributed system. This test is basically for testing the 
// effect of querying on various cache operations. It ensures that
// the querying works properly without hang in distributed environment
INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_1_locator.inc;
INCLUDE $JTESTS/util/randomValues.inc;

hydra.ClientPrms-vmQuantities = 1 1 1 1 1;
hydra.ClientPrms-vmThreads    = 1 1 3 3 3;

THREADGROUP locatorThreads
  totalThreads = fcn
                 ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"locator\", ${locatorHosts}, true)"
                 ncf;

THREADGROUP client1Thread  totalThreads = 3 clientNames  = client1;
THREADGROUP client2Thread  totalThreads = 3 clientNames  = client2;
THREADGROUP client3Thread  totalThreads = 3 clientNames  = client3;


hydra.VmPrms-extraClassPaths        =   $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar, 
                                        $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar,
                                        $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar,
                                        $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar;

STARTTASK    taskClass   = event.EventBB taskMethod  = HydraTask_initialize
             clientNames = client1;
STARTTASK    taskClass   = query.QueryBB taskMethod  = HydraTask_initialize
             clientNames = client1;
STARTTASK    taskClass   = util.CacheBB taskMethod  = HydraTask_initialize
             clientNames = client1;

TASK         taskClass   = query.QueryTest taskMethod  = HydraTask_doEntryAndQueryOperations
             threadGroups = client1Thread, client2Thread, client3Thread;

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 300;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-serialExecution = true;
hydra.Prms-alwaysDoEndTasks = false;
hydra.Prms-maxClientShutdownWaitSec = 240;
hydra.Prms-finalClientSleepSec = 60;
hydra.Prms-maxEndTaskResultWaitSec = 400;

// Logging params
hydra.log.LogPrms-file_logging = true;

util.RandomValuesPrms-borderCasePercentage = 0;
util.RandomValuesPrms-objectType = java.lang.Integer;
util.RandomValuesPrms-valueType = java.lang.Integer;

util.TestHelperPrms-printElementsLimit = 0;
util.TestHelperPrms-printStringLimit = 100;
util.TestHelperPrms-printObjectDepth = 0;
util.ValueHolderPrms-useExtraObject = false;
event.EventPrms-useRandomLocks = false;
event.EventPrms-maxObjects = 100;

query.QueryPrms-entryAndQueryOperations = ONEOF add add add add destroy destroy query query FOENO; // weighted to adds
query.QueryPrms-objectType = objects.Portfolio;
query.QueryPrms-numOfRegions = 3;
util.TestHelperPrms-minTaskGranularitySec = 60;

