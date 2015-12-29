hydra.GemFirePrms-offHeapMemorySize=350m;               // this sets off-heap memory for older tests (ignored in newer tests)
hydra.gemfirexd.FabricServerPrms-offHeapMemorySize=350m;  // this sets off-heap memory for newer tests (ignored in older tests)
sql.SQLPrms-isOffheap = true;                           // true means all tables are offheap (via offheap keyword on create table statement)

// verify off-heap memory at the end of the test
// Don't use the lifecycle listener on rolling upgrade tests because this finds bug 48459 (orphans)
// in GemFireXD 1.0 members, a bug we know we had in 1.0. 
// Check for orphans at the end of the test, which will be members at the current GemFire XD version
CLOSETASK taskClass = util.AEQHelper taskMethod = waitForAsyncEventQueuesToDrain;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = waitForOffHeapSilence;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = closeAllOffHeapRegions;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;

