hydra.Prms-testRequirement = 
"Test entry put/get/update events on cache regions in 
muti-threaded and transactional environment";

hydra.Prms-testDescription = "This test concurrently executes 
put/get/update operation on cache regions in multi-threded transactional environment";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparams1.inc;

hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 2;

hydra.VmPrms-extraClassPaths        =   
$GEMFIRE/lib/gemfire.jar $JTESTS/lib/derby.jar,$GEMFIRE/lib/gemfire.jar $JTESTS/lib/derby.jar,$GEMFIRE/lib/gemfire.jar $JTESTS/lib/derby.jar; 

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 1200;
hydra.Prms-maxResultWaitSec = 1200;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-serialExecution = false;
hydra.Prms-alwaysDoEndTasks = false;
hydra.Prms-maxClientShutdownWaitSec = 360;
hydra.Prms-finalClientSleepSec = 60;
hydra.Prms-maxEndTaskResultWaitSec = 1800;

// Logging params
hydra.log.LogPrms-file_logging = true;

jta.JtaPrms-numberOfRandomRegions=0;
jta.JtaPrms-entryOperations = ONEOF put update get FOENO; 

jta.JtaPrms-cacheXmlFile = $JTESTS/jta/cachejta.xml;

