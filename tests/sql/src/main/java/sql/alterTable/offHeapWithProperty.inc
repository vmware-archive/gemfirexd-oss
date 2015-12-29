hydra.GemFirePrms-offHeapMemorySize=350m;               // this sets off-heap memory for older tests (ignored in newer tests)
hydra.gemfirexd.FabricServerPrms-offHeapMemorySize=350m;  // this sets off-heap memory for newer tests (ignored in older tests)
// a few tests are structured such that it is difficult to randomize off-heap
// and get the off-heap keyword onto the create table statement (it uses scripts and
// those scripts are used in a method that creates the tables in both derby and gfxd
// but putting the offheap keyword in the scripts would cause the derby part to fail);
// just use the test hook property to enable off-heap in these tests as the offheap
// keyword is covered in many other tests
hydra.VmPrms-extraVMArgs += "-Dgemfirexd.TEST_FLAG_OFFHEAP_ENABLE=true";


// verify off-heap memory at the end of the test and when members are taken down
INITTASK taskClass = memscale.OffHeapMemoryLifecycleListener taskMethod = install runMode=always;
CLOSETASK taskClass = util.AEQHelper taskMethod = waitForAsyncEventQueuesToDrain;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = waitForOffHeapSilence;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = closeAllOffHeapRegions;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;
ENDTASK taskClass = memscale.OffHeapMemoryLifecycleListener taskMethod = checkForErrors;

