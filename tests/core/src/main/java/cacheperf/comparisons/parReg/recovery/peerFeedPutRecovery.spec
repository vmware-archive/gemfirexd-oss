include $JTESTS/cacheperf/gemfire/specs/creates.spec;
include $JTESTS/cacheperf/gemfire/specs/extraputs.spec;
include $JTESTS/cacheperf/comparisons/parReg/recovery/recovery.spec;

// need these to be specific to feeds to avoid issues

statspec putsPerSecond feed* cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalPuts feed* cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
statspec totalPutTime feed* cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
expr putResponseTime = totalPutTime / totalPuts ops=max-min?
;
