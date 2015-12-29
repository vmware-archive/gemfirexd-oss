include $JTESTS/cacheperf/comparisons/creates.spec;
include $JTESTS/cacheperf/comparisons/puts.spec;

statspec nulls * cacheperf.CachePerfStats * nulls
filter=none combine=combineAcrossArchives ops=max? trimspec=untrimmed
;
