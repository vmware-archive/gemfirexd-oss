include $JTESTS/cacheperf/comparisons/creategets.spec;

statspec nulls * cacheperf.CachePerfStats * nulls
filter=none combine=combineAcrossArchives ops=max? trimspec=untrimmed
;
