// End tasks for persistent bridge tests; recover from disk and validate recovery 
hydra.Prms-doStartAndEndTasksLockStep = true;

// prepare
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_prepareForRecovery
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"bridge\", ${bridgeHosts} * ${bridgeVMsPerHost}, true)" ncf;

// start servers and validate region contents while async redundancy recovery is running
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeDataStore
            parReg.ParRegPrms-recoverFromDisk = true
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"bridge\", ${bridgeHosts}, true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"bridge\", ${bridgeHosts}, true)" ncf;

// start edge clients, verify their region contents
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeAccessor
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"edge\", ${edgeHosts}, true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"edge\", ${edgeHosts}, true)" ncf;

// wait for recovery to complete then verify PR internals
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"bridge\", ${bridgeHosts} * ${bridgeVMsPerHost}, true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateInternalPRState
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"bridge\", ${bridgeHosts} * ${bridgeVMsPerHost}, true)" ncf;

// prepare for offline validation and compaction
ENDTASK     taskClass   = parReg.ParRegTest     taskMethod = HydraTask_disconnect; 
ENDTASK     taskClass   = util.PersistenceUtil  taskMethod = HydraTask_doOfflineValAndCompactionOnce;

// validate after compaction; validate the region contents while async redundancy recovery is running
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_prepareForRecovery;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeDataStore
            parReg.ParRegPrms-recoverFromDisk = true
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"bridge\", ${bridgeHosts}, true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"bridge\", ${bridgeHosts}, true)" ncf;

// validate after compaction; start edge clients, verify their region contents
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeAccessor
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"edge\", ${edgeHosts}, true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"edge\", ${edgeHosts}, true)" ncf;

// validate after compaction; wait for recovery to complete then verify PR internals
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"bridge\", ${bridgeHosts} * ${bridgeVMsPerHost}, true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateInternalPRState
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"bridge\", ${bridgeHosts} * ${bridgeVMsPerHost}, true)" ncf;


