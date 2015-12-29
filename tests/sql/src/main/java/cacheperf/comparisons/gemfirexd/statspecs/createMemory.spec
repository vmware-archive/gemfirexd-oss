include $JTESTS/cacheperf/comparisons/gemfirexd/statspecs/create.spec;
include $JTESTS/cacheperf/comparisons/gemfirexd/statspecs/memory.spec;

statspec totalCreates *client* QueryPerfStats * creates
filter=none combine=combineAcrossArchives ops=min,max?,mean,stddev trimspec=none
;
expr memoryUsedPerCreate = totalMemoryUsed / totalCreates ops=max?
;

