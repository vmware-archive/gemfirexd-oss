include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/systemparams2.inc;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          ;
INITTASK  taskClass = query.index.IndexTest taskMethod = HydraTask_CreateIndex
          ;

CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          ;

hydra.ClientPrms-vmQuantities = 1 1;
hydra.ClientPrms-vmThreads    = 2 2;
hydra.VmPrms-extraClassPaths        =   $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar, 
                                        $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar;
//cacheperf.CachePerfPrms-batchSize      =  2500000;
//cacheperf.CachePerfPrms-trimIterations =  5000000;
//cacheperf.CachePerfPrms-workIterations = 10000000;
//cacheperf.CachePerfPrms-maxKeys        =  1000000;

cacheperf.CachePerfPrms-batchSize      =  2500;
cacheperf.CachePerfPrms-trimIterations =  5000;
cacheperf.CachePerfPrms-workIterations = 10000;
cacheperf.CachePerfPrms-maxKeys        =  1000;
cacheperf.CachePerfPrms-objectType     = objects.Portfolio;
objects.SizedStringPrms-constant = true;
hydra.ConfigPrms-cacheConfig = cache;
hydra.ConfigPrms-regionConfig = region;
distcache.gemfire.GemFireCachePrms-regionName = IndexRegion;
hydra.CachePrms-names            = cache;
hydra.RegionPrms-names           = region;
hydra.RegionPrms-dataPolicy      = replicated;
hydra.RegionPrms-scope           = global;
hydra.RegionPrms-valueConstraint = objects.Portfolio;
hydra.Prms-totalTaskTimeSec = 60000;
hydra.Prms-maxResultWaitSec = 300;
