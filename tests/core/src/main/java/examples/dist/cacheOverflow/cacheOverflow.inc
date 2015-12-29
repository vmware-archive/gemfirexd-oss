// General include file for tests that test the CacheOverflow example

include $JTESTS/hydraconfig/hydraparams1.inc;

hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 1;
hydra.VmPrms-extraClassPaths = $GEMFIRE/examples/dist/classes;
hydra.log.LogPrms-mergeLogFiles = onFailure;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

TASK
  taskClass = examples.dist.cacheOverflow.CacheOverflowTasks
  taskMethod = populateTask
  MAXTIMESTORUN = 1
  ;

