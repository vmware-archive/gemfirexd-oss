include $JTESTS/cacheperf/gemfire/specs/process.spec;
include $JTESTS/cacheperf/gemfire/specs/cacheOpens.spec;

//------------------------------------------------------------------------------
// Statistics Specifications for Put Comparison for getInitialImage
//------------------------------------------------------------------------------

statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=cacheOpens
;
statspec totalPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=cacheOpens
;
statspec totalPutTime * cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=cacheOpens
;
expr putResponseTime = totalPutTime / totalPuts ops=max-min?
;
