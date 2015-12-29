include $JTESTS/smoketest/perf/common.spec
;
statspec putGetsPerSecond * cacheperf.CachePerfStats * combinedPutGets
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=combinedputgets
;
