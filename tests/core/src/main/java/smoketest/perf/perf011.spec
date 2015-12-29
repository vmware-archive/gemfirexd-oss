include $JTESTS/smoketest/perf/common.spec
;
statspec locksPerSecond * cacheperf.CachePerfStats * locks
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=locks
;
