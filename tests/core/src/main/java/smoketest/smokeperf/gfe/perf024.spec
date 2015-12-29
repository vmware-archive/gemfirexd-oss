include $JTESTS/smoketest/smokeperf/gfe/common.spec
;
statspec createsPerSecond * cacheperf.CachePerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
statspec destroysPerSecond * CachePerfStats cachePerfStats destroys
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
statspec offHeapReadsPerSecond * OffHeapMemoryStats * reads
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
statspec offHeapObjects * OffHeapMemoryStats * objects
filter=none combine=combineAcrossArchives ops=max? trimspec=creates
;
