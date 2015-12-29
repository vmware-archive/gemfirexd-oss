include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_p2p_2.inc;

hydra.Prms-testDescription = "Scales the number of peers connected to a distributed system.  Each member connects, then checks the number of members, then creates a cache, then half create one partitioned region and the other half create another partitioned region, then everyone puts data while a VM bounces itself every so often.";
hydra.Prms-testRequirement = "The number of members possible is very large.";

THREADGROUP prOne
    totalThreads = fcn
                   ${prOneHosts} * ${prOneVMsPerHost} * ${prOneThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"prOne\", ${prOneHosts}, true)"
                   ncf;
THREADGROUP prTwo
    totalThreads = fcn
                   ${prTwoHosts} * ${prTwoVMsPerHost} * ${prTwoThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"prTwo\", ${prTwoHosts}, true)"
                   ncf;

//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

hydratest.locators.LocatorPrms-expectedMembers =
          fcn ${prOneHosts} * ${prOneVMsPerHost} +
              ${prTwoHosts} * ${prTwoVMsPerHost} ncf
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = connectTask
          threadgroups = prOne, prTwo
          runMode = always
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          threadgroups = prOne, prTwo
          runMode = always
          ;
INITTASK  taskClass = hydratest.locators.LocatorClient
          taskMethod = validateExpectedMembersTask
          threadgroups = prOne, prTwo
          runMode = always
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = cache
          hydra.ConfigPrms-regionConfig = region
          distcache.gemfire.GemFireCachePrms-regionName = prOne
          runMode = always
          threadgroups = prOne
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = cache
          hydra.ConfigPrms-regionConfig = region
          distcache.gemfire.GemFireCachePrms-regionName = prTwo
          runMode = always
          threadgroups = prTwo
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createDataTask
          batch
          cacheperf.CachePerfPrms-keyAllocation = ownKeys
          cacheperf.CachePerfPrms-warmupTerminatorMethod = none
          cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnMaxKey
          cacheperf.CachePerfPrms-taskTerminatorFrequency = 1 iterations
          threadgroups = prOne, prTwo
          ;
TASK      taskClass = cacheperf.CachePerfClient taskMethod = putDataTask
          cacheperf.CachePerfPrms-keyAllocation = sameKeysRandomWrap
          threadgroups = prOne, prTwo
          ;
TASK      taskClass = cacheperf.CachePerfClient taskMethod = bounceTask
          maxThreads = 1
          threadgroups = prOne, prTwo
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          runMode = always
          threadgroups = prOne, prTwo
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          runMode = always
          threadgroups = prOne, prTwo
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = disconnectTask
          runMode = always
          threadgroups = prOne, prTwo
          ;

//------------------------------------------------------------------------------
// Cache and region
//------------------------------------------------------------------------------

hydra.CachePrms-names = cache;
hydra.RegionPrms-names = region;
//hydra.RegionPrms-scope = default;
hydra.RegionPrms-dataPolicy = partitioned;
hydra.RegionPrms-partitionName = pr;
hydra.PartitionPrms-names = pr;
hydra.PartitionPrms-redundantCopies = 1;

//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-maxKeys    = 113000;
cacheperf.CachePerfPrms-objectType = objects.ArrayOfByte;
objects.ArrayOfBytePrms-size       = 100;

//------------------------------------------------------------------------------

hydra.Prms-maxResultWaitSec = 900;
hydra.Prms-totalTaskTimeSec = ${workSeconds};
cacheperf.CachePerfPrms-restartWaitSec = ${restartWaitSec};
cacheperf.CachePerfPrms-sleepMs = fcn ${sleepSec} * 1000 ncf;

cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds              = 30;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 5 seconds;
cacheperf.CachePerfPrms-trimSeconds               = 10;
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 5 seconds;
cacheperf.CachePerfPrms-workSeconds               = ${workSeconds};

//------------------------------------------------------------------------------
// helps mitigate Bug 39849 and possibly Bug 39875

hydra.VmPrms-extraVMArgs += "-Dp2p.HANDSHAKE_POOL_SIZE=${handshakePoolSize}";
