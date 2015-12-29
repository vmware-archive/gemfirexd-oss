// test multithreaded use of the UnsharedLRUEntriesMap with a LRUCapacityController set at 1000 entries.

include $JTESTS/hydraconfig/systemparams1.inc;
include $JTESTS/hydraconfig/hydraparams1.inc;

hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 10;

hydra.Prms-totalTaskTimeSec = 120;
hydra.log.LogPrms-mergeLogFiles = true;

// one of MemLRU or LRU, default is LRU
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerType = LRU;

// should have an integer value that corresponds to LRUCapacityController's maximum-entries
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerEntries = oneof 1000 1200 1500 2000 foeno;

// should have an integer value that corresponds to MemLRUCapacityController's maximum-megabytes
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerMegabytes = oneof 1 2 3 foeno;

// the size of the byte array to use as the entrie's value.
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-entryValueSize = oneof 1000 1000 1000 100 2000 2000 5000 10000 foeno;

// use region or test directly at a map.
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-useRegion = false;

com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-sleepTime = 0;

INITTASK   taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper    
           taskMethod  = setup;

TASK       taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper     
           taskMethod  = put1kNew
           WEIGHT = 20;

TASK       taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper     
           taskMethod  = put10kNew
           WEIGHT = 5;

TASK       taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper     
           taskMethod  = replace100
           WEIGHT = 10;

TASK       taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper     
           taskMethod  = remove100
           WEIGHT = 10;

TASK       taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper     
           taskMethod  = checkCounter
           MAXTHREADS = 1
           WEIGHT = 1;

TASK       taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper
           taskMethod  = changeLimit
           MAXTHREADS = 1
           WEIGHT = 1;

CLOSETASK  taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper
           taskMethod  = checkMapSize;
           


           