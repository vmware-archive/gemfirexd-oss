include $JTESTS/smoketest/smokeperf/gfe/common.spec
;
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=putgets
;
statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=putgets
;
statspec offHeapReadsPerSecond * OffHeapMemoryStats * reads
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=putgets
;
statspec offHeapObjects * OffHeapMemoryStats * objects
filter=none combine=combineAcrossArchives ops=max? trimspec=putgets
;
