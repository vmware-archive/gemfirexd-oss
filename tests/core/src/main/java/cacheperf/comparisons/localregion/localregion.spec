include $JTESTS/cacheperf/comparisons/creates.spec;
include $JTESTS/cacheperf/comparisons/putgets.spec;

statspec nulls * cacheperf.CachePerfStats * nulls
filter=none combine=combineAcrossArchives ops=max? trimspec=untrimmed
;
