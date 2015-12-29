include $JTESTS/smoketest/smokeperf/gfe/common.spec
;
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
statspec evictionsPerSecond * LRUStatistics * lruEvictions
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;
