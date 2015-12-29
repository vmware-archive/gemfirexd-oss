include $JTESTS/cacheperf/gemfire/specs/creates.spec;
include $JTESTS/cacheperf/gemfire/specs/extragets.spec;
include $JTESTS/cacheperf/comparisons/parReg/recovery/recovery.spec;

// need these to be specific to feeds to avoid issues

statspec getsPerSecond feed* cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=gets
;
statspec totalGets feed* cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=gets
;
statspec totalGetTime feed* cacheperf.CachePerfStats * getTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=gets
;
expr getResponseTime = totalGetTime / totalGets ops=max-min?
;
