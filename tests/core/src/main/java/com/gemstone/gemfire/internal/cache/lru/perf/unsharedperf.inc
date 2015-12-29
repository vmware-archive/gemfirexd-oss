// test multithreaded use of the SharedLRUEntriesMap with a LRUCapacityController set at 1000 entries.

include $JTESTS/hydraconfig/systemparams1.inc;
include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/perftest/perfstats.inc;

hydra.ClientPrms-vmQuantities = ${vmQuantities};
hydra.ClientPrms-vmThreads    = ${vmThreads};

perftest.PerfPrms-trimIterations = ${trimIterations};
perftest.PerfPrms-workIterations = ${workIterations};

hydra.Prms-totalTaskTimeSec      = 1;    // Only schedule tasks once
hydra.Prms-maxResultWaitSec      = 300;  // five minutes.

hydra.log.LogPrms-mergeLogFiles = true;

// dummy values for TestHelper
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerType = LRU;
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerEntries = 2000;
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-capacityControllerMegabytes = 1;

// the size of the byte array to use as the entrie's value.
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-entryValueSize = 100;

// use region (true) or test directly at a map (false).
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-useRegion = true;

com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-sleepTime = 0;

// use local scope for region
com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper-useLocalScope = true;


INITTASK   taskClass   = perftest.PerfClient
           taskMethod  = openStatisticsTask;

INITTASK   taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper    
           taskMethod  = setupNoCC;

TASK       taskClass   = com.gemstone.gemfire.internal.cache.UnsharedLRUEntriesMapTestHelper     
           taskMethod  = putNoCCPerf;
           
CLOSETASK  taskClass   = perftest.PerfClient
           taskMethod  = closeStatisticsTask;


           
