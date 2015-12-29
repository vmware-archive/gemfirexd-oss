hydra.gemfirexd.FabricServerPrms-offHeapMemorySize=350m;  
sql.SQLPrms-sqlFilePath = sql/poc/useCase2/createTablesOffHeap.sql;	

// verify off-heap memory at the end of the test and when members are taken down
INITTASK taskClass = memscale.OffHeapMemoryLifecycleListener taskMethod = install runMode=always;
CLOSETASK taskClass = util.AEQHelper taskMethod = waitForAsyncEventQueuesToDrain;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = waitForOffHeapSilence;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = closeAllOffHeapRegions;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;
ENDTASK taskClass = memscale.OffHeapMemoryLifecycleListener taskMethod = checkForErrors;

