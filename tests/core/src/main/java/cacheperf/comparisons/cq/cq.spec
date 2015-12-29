include $JTESTS/cacheperf/specs/ops.spec;
include $JTESTS/cacheperf/specs/opResponseTime.spec;
include $JTESTS/cacheperf/specs/updateLatency.spec;

statspec createsPerSecond * cacheperf.CachePerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
statspec memoryUse * ProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=puts
;
statspec memoryGrowth * ProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max-min? trimspec=puts
;
statspec loadAverage * SystemStats * loadAverage1
filter=none combine=combineAcrossArchives ops=min,max,mean? trimspec=puts
;
