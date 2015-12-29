INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
INCLUDE $JTESTS/hydraconfig/systemparams2.inc;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask;

CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask;

CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask;

CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask;

hydra.ClientPrms-vmQuantities = 1;  // per gemfire system
hydra.ClientPrms-vmThreads    = 30; // with 2 gemfire systems, 1 VM per gf system, 60 threads total

hydra.GemFirePrms-conserveSockets               =   true;

hydra.Prms-totalTaskTimeSec = 60000;
hydra.Prms-maxResultWaitSec = 600;

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/evict/evict.spec;


// keys/values
cacheperf.CachePerfPrms-keyAllocation             = ownKeys;
cacheperf.CachePerfPrms-objectType                = objects.ArrayOfByte;
cacheperf.CachePerfPrms-maxKeys                   = 999999999;
objects.ArrayOfBytePrms-size                      = 5000;

// cache and region configuration
hydra.ConfigPrms-cacheConfig = cache;
hydra.CachePrms-names = cache;
hydra.ConfigPrms-regionConfig = region;
hydra.RegionPrms-names = region;
hydra.RegionPrms-scope = distributedNoAck;
hydra.RegionPrms-cacheLoader = cacheperf.gemfire.SleepLoader;
hydra.RegionPrms-initialCapacity = 200000;
hydra.RegionPrms-concurrencyLevel = 120;

// sleep settings
cacheperf.gemfire.GemFireCachePrms-loaderSleepMs = 15;
cacheperf.CachePerfPrms-syncSleepMs = 30000;
