// End tasks for persistent peer tests; recover from disk and validate recovery 
hydra.Prms-doStartAndEndTasksLockStep = true;

// recover from disk
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeDataStore
            parReg.ParRegPrms-recoverFromDisk = true
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"dataStore\", ${dataStoreHosts}, true)" ncf;
//start accessors after recovering from disk.            
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeAccessor
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"accessor\", ${accessorHosts}, true)" ncf;

// validate region contents while redundancy recovery is running,
// wait for redundancy recovery and verify PR internals 
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"dataStore\", ${dataStoreHosts}, true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateInternalPRState;

// offline validation and compaction
ENDTASK     taskClass   = parReg.ParRegTest     taskMethod = HydraTask_disconnect; 
ENDTASK     taskClass   = util.PersistenceUtil  taskMethod = HydraTask_doOfflineValAndCompactionOnce;

// recover from disk
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_prepareForRecovery;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeDataStore
            parReg.ParRegPrms-recoverFromDisk = true
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"dataStore\", ${dataStoreHosts}, true)" ncf;
//start accessors after recovering from disk.            
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeAccessor
            clientNames = fcn "hydra.TestConfigFcns.generateNames (\"accessor\", ${accessorHosts}, true)" ncf;

// validate after compaction; validate region contents while redundancy recovery is running,
// wait for redundancy recovery and verify PR internals 
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery
            clientNames = fcn "hydra.TestConfigFcns.generateNames(\"dataStore\", ${dataStoreHosts}, true)" ncf;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateInternalPRState;

