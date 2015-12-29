// Servers need antlr jar for query support
hydra.VmPrms-extraClassPaths =
   fcn "hydra.TestConfigFcns.duplicate(\"$GEMFIRE/lib/antlr.jar\", ${bridgeHosts}, true)" ncf;

// notifyBySubscription must be true for CQs

hct.HctPrms-receiveValuesAsInvalidates = false;

// establishCallbackConnect must be true for CQs
hydra.PoolPrms-subscriptionEnabled = true;

cq.CQUtilPrms-useCQ = true;
cq.CQUtilPrms-cqListener = cq.CQTestListener;
cq.CQUtilPrms-QueryServicePoolName = brclient;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

// Since we cannot evict in the servers (replicates cannot evict),
// don't create as much data during the test
hct.HctPrms-hitsPerMiss = 24;
