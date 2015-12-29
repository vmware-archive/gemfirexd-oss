// End tasks for persistent peer tests; recover from disk and validate recovery 
hydra.Prms-doStartAndEndTasksLockStep = true;

// recover from disk 
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_initialize
            parReg.ParRegPrms-recoverFromDisk = true;

// validate region contents while redundancy recovery is running,
// wait for redundancy recovery and verify PR internals 
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateInternalPRState;

// offline validation and compaction
ENDTASK     taskClass   = parReg.ParRegTest     taskMethod = HydraTask_disconnect; 
ENDTASK     taskClass   = util.PersistenceUtil  taskMethod = HydraTask_doOfflineValAndCompactionOnce;

// recover from disk
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_prepareForRecovery;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_initialize
            parReg.ParRegPrms-recoverFromDisk = true;

// validate after compaction; validate region contents while redundancy recovery is running,
// wait for redundancy recovery and verify PR internals 
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateRegionContents;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery;
ENDTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validateInternalPRState;


