include $JTESTS/smoketest/smokeperf/gfe/common.spec
;
statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=gets
;
statspec offHeapReadsPerSecond * OffHeapMemoryStats * reads
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=gets
;
statspec offHeapObjects * OffHeapMemoryStats * objects
filter=none combine=combineAcrossArchives ops=max? trimspec=gets
;
