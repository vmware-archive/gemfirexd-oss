include $JTESTS/cacheperf/gemfire/specs/process.spec;
include $JTESTS/cacheperf/gemfire/specs/cacheOpens.spec;

//------------------------------------------------------------------------------
// Statistics Specifications for Get Comparison for getInitialImage
//------------------------------------------------------------------------------

statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=cacheOpens
;
statspec totalGets * cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=cacheOpens
;
statspec totalGetTime * cacheperf.CachePerfStats * getTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=cacheOpens
;
expr getResponseTime = totalGetTime / totalGets ops=max-min?
;
