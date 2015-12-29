include $JTESTS/smoketest/perf/common.spec
;
statspec createsPerSecond * cacheperf.CachePerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
statspec evictionsPerSecond * LRUStatistics * lruEvictions
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=creates
;
