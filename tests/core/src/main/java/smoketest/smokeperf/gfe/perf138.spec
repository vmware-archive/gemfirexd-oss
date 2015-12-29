include $JTESTS/smoketest/smokeperf/gfe/common.spec
;
statspec createsPerSecond * cacheperf.CachePerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
statspec evictionsPerSecond * LRUStatistics * lruEvictions
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
statspec offHeapReadsPerSecond * OffHeapMemoryStats * reads
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
statspec offHeapObjects * OffHeapMemoryStats * objects
filter=none combine=combineAcrossArchives ops=max? trimspec=puts
;
