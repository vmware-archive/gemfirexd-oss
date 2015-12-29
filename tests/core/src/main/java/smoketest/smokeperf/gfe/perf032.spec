include $JTESTS/smoketest/smokeperf/gfe/common.spec
;
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
statspec diskWritesPerSecond * DiskRegionStatistics * writes
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
statspec offHeapReadsPerSecond * OffHeapMemoryStats * reads
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
statspec offHeapObjects * OffHeapMemoryStats * objects
filter=none combine=combineAcrossArchives ops=max? trimspec=puts
;
