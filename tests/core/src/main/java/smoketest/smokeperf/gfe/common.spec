include $JTESTS/cacheperf/specs/ops.spec
;
include $JTESTS/cacheperf/specs/opResponseTime.spec
;
statspec fdsOpen * VMStats * fdsOpen
filter=none combine=combineAcrossArchives ops=max-min? trimspec=none
;
statspec memoryGrowth * VMMemoryUsageStats vmHeapMemoryStats usedMemory
filter=none combine=combineAcrossArchives ops=mean? trimspec=operations
;
statspec loadAverage * SystemStats * loadAverage1
filter=none combine=combineAcrossArchives ops=min,max,mean? trimspec=operations
;
