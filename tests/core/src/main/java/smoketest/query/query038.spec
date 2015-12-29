include $JTESTS/smoketest/perf/common.spec
;
statspec destroysPerSecond * cacheperf.CachePerfStats * destroys
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=destroys
;
