hydra.Prms-testRequirement = "Test  query/create/update/destroy/invalidate events";
hydra.Prms-testDescription = "
Test validate entry operations and querying.

The test uses one region, a distributed scope and mirroring of keys
and values. There is a listener, that increments a blackboard counter
for each event, installed in the region for each VM.

The test executes a task that creates/updates/invalidates/destroys/query
in a region, then waits for the listener counters to reach
the required number, indicating the event was received in a VM.
";
// A Hydra test that performs a number of cache-related operations and 
// querying in a distributed system. This test is basically for testing the 
// effect of querying on various cache operations. It ensures that
// the querying works properly without hang in distributed environment
INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparams3.inc;

INCLUDE $JTESTS/util/randomValues.inc;

hydra.ClientPrms-vmQuantities = 2 2 2;
hydra.ClientPrms-vmThreads    = 3 3 3;

hydra.VmPrms-extraClassPaths        =   $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar, 
                                        $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar,
                                        $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar;
STARTTASK    taskClass   = event.EventBB taskMethod  = HydraTask_initialize
             clientNames = client1;
STARTTASK    taskClass   = query.QueryBB taskMethod  = HydraTask_initialize
             clientNames = client1;
STARTTASK    taskClass   = util.CacheBB taskMethod  = HydraTask_initialize
             clientNames = client1;

TASK         taskClass   = query.QueryTest taskMethod  = HydraTask_doEntryAndQueryOperations;

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 500;
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
util.TestHelperPrms-minTaskGranularitySec = 60;

