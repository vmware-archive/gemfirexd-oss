include $JTESTS/cacheperf/specs/ops.spec
;
include $JTESTS/cacheperf/specs/opResponseTime.spec
;
statspec fdsOpen * VMStats * fdsOpen
filter=none combine=combineAcrossArchives ops=max? trimspec=none
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
