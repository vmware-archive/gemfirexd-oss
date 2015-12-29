include $JTESTS/smoketest/smokeperf/gfe/common.spec
;
statspec putGetsPerSecond * cacheperf.CachePerfStats * combinedPutGets
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=combinedputgets
;
statspec offHeapReadsPerSecond * OffHeapMemoryStats * reads
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=combinedputgets
;
statspec offHeapObjects * OffHeapMemoryStats * objects
filter=none combine=combineAcrossArchives ops=max? trimspec=combinedputgets
;
