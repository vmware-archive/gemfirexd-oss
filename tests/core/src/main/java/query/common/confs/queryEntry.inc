hydra.Prms-testRequirement = "Test  query/create/update/destroy/invalidate events";
hydra.Prms-testDescription = "
Test validate entry operations and querying.
";
// A Hydra test that performs a number of cache-related operations and 
// querying in a distributed system. This test is basically for testing the 
// effect of querying on various cache operations. It ensures that
// the querying works properly without hang in distributed environment
INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparams3.inc;

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
STARTTASK    taskClass   = query.index.IndexBB taskMethod  = HydraTask_initialize
             clientNames = client1;
             
TASK         taskClass   = query.generalized.QueryTest taskMethod  = HydraTask_doEntryAndQueryOperations;

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 500;
util.TestHelperPrms-minTaskGranularitySec = 60;

// Logging params
hydra.log.LogPrms-file_logging = true;

event.EventPrms-maxObjects = 100;

query.generalized.QueryPrms-entryAndQueryOperations = ONEOF add add add add destroy destroy query query FOENO; // weighted to adds

