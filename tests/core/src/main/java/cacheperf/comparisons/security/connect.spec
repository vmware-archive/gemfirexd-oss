// this is not a throughput test -- operations are throttled
include $JTESTS/cacheperf/specs/opResponseTime.spec
;
statspec memoryUse * ProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max? trimspec=operations
;
statspec memoryGrowth * ProcessStats * rssSize
filter=none combine=combineAcrossArchives ops=max-min? trimspec=operations
;
statspec loadAverage * SystemStats * loadAverage1
filter=none combine=combineAcrossArchives ops=min,max,mean? trimspec=operations
;
