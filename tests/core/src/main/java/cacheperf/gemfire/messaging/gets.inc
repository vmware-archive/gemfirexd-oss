include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/systemparamsN.inc;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          threadGroups = putter, getter

          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          distcache.gemfire.GemFireCachePrms-regionName=WorkCache
          threadGroups = putter
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createDataTask
          threadGroups = putter
	  batch
          ;
TASK      taskClass = cacheperf.CachePerfClient taskMethod = putDataTask
          threadGroups = putter
          cacheperf.CachePerfPrms-workIterations = 225000
          ;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          distcache.gemfire.GemFireCachePrms-regionName=WorkCache
          threadGroups = getter
          ;
TASK      taskClass = cacheperf.CachePerfClient taskMethod = getDataTask
          threadGroups = getter
          ;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          threadGroups = extraCache
          ;

CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = putter, getter
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          threadGroups = putter, getter
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          threadGroups = putter, getter
          ;

hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 1;

THREADGROUP putter totalThreads = 1 totalVMs = 1 clientNames = client1;
THREADGROUP getter totalThreads = 1 totalVMs = 1 clientNames = client2;
THREADGROUP extraCache totalThreads = ${numExtra} totalVMs = ${numExtra};

hydra.ConfigPrms-cacheConfig = cache;
hydra.CachePrms-names = cache;

hydra.ConfigPrms-regionConfig = region;
hydra.RegionPrms-names = region;
hydra.RegionPrms-scope = distributedAck;

cacheperf.CachePerfPrms-trimIterations =  25000;
cacheperf.CachePerfPrms-workIterations = 100000;
cacheperf.CachePerfPrms-maxKeys        = 125000;
cacheperf.CachePerfPrms-batchSize      =   5000;
cacheperf.CachePerfPrms-objectType     = objects.SizedString;
objects.SizedStringPrms-size           = range 50 100 egnar;

hydra.Prms-totalTaskTimeSec = 60000;
hydra.Prms-maxResultWaitSec = 300;

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/messaging/msgperfstats.spec;
