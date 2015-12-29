// test multithreaded use of the UnsharedLRUEntriesMap with a LRUCapacityController set at 1000 entries.

include $JTESTS/hydraconfig/systemparams1.inc;
include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/perftest/perfstats.inc;

hydra.ClientPrms-vmQuantities = ${vmQuantities};
hydra.ClientPrms-vmThreads    = ${vmThreads};

perftest.PerfPrms-trimIterations = ${trimIterations};
perftest.PerfPrms-workIterations = ${workIterations};

hydra.Prms-totalTaskTimeSec      = 1;    // Only schedule tasks once
hydra.Prms-maxResultWaitSec      = 180;  // three minutes.

hydra.log.LogPrms-mergeLogFiles = true;

// one of MemLRU or LRU, default is LRU
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerType = LRU;

// should have an integer value that corresponds to LRUCapacityController's maximum-entries
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerEntries = 2000;

// should have an integer value that corresponds to MemLRUCapacityController's maximum-megabytes
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerMegabytes = 1;

// the size of the byte array to use as the entrie's value.
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-entryValueSize = 100;

// use region or test directly at a map.
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-useRegion = true;

// use local scope for region
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-useLocalScope = true;


INITTASK   taskClass   = perftest.PerfClient
           taskMethod  = openStatisticsTask;

INITTASK   taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper    
           taskMethod  = setup;

TASK       taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper     
           taskMethod  = putPerf;
           
CLOSETASK  taskClass   = perftest.PerfClient
           taskMethod  = closeStatisticsTask;


           
