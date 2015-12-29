include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/systemparams2.inc;

hydra.Prms-testDescription = " Test behavior of secondary (distNoAck, keysValues, cacheListener) when primary (distNoAck, keysValues, persist) is killed while processing thousands of updates per second";

hydra.Prms-testRequirement = " TBD ";

INITTASK  taskClass = useCase13Scenarios.UseCase13Client taskMethod = openStatisticsTask
          ;
INITTASK  taskClass = useCase13Scenarios.UseCase13Client taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = primary
          hydra.ConfigPrms-regionConfig = primary
          threadGroups = primary
          ;
INITTASK  taskClass = useCase13Scenarios.UseCase13Client taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = secondary
          hydra.ConfigPrms-regionConfig = secondary
          threadGroups = secondary
          ;
TASK      taskClass = useCase13Scenarios.UseCase13Client taskMethod = putDataTask
	  cacheperf.CachePerfPrms-keyAllocation = sameKeysWrap
          cacheperf.CachePerfPrms-warmupTerminatorMethod = terminateOnTrimIterations 
	  cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnTotalIterations
	  cacheperf.CachePerfPrms-taskTerminatorFrequency = 60 seconds
          threadGroups = primary
          ;

TASK      taskClass = useCase13Scenarios.UseCase13Client taskMethod = killAfterMinOps
          threadGroups = killer
          ;

CLOSETASK taskClass = useCase13Scenarios.UseCase13Client taskMethod = closeCacheTask
          threadGroups = primary, secondary;
          ;
CLOSETASK taskClass = useCase13Scenarios.UseCase13Client taskMethod = closeStatisticsTask
          ;
CLOSETASK taskClass = useCase13Scenarios.UseCase13Client taskMethod = reportTrimIntervalsTask
          ;

THREADGROUP primary totalThreads = 1 totalVMs = 1 clientNames = client1;
THREADGROUP secondary  totalThreads = 1 totalVMs = 1 clientNames = client2;

hydra.Prms-haltIfBadResult = false;
hydra.Prms-maxResultWaitSec = 180;  // 3 minutes
hydra.Prms-totalTaskTimeSec = 300;  // 5 minutes

hydra.ClientPrms-vmQuantities = 1 1;

hydra.VmPrms-extraVMArgs = -Xmx750m;

cacheperf.CachePerfPrms-trimIterations = ${trimIterations};
cacheperf.CachePerfPrms-workIterations = ${workIterations};
cacheperf.CachePerfPrms-batchSize      = ${batchSize};
cacheperf.CachePerfPrms-maxKeys        = ${maxKeys};
cacheperf.CachePerfPrms-objectType = objects.ArrayOfByte;
objects.ArrayOfBytePrms-size = range 10 1024 egnar;

cacheperf.gemfire.GemFireCachePrms-processListenerEvents = true;
cacheperf.gemfire.GemFireCachePrms-logListenerEvents = true;

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/specs/puts.spec;
