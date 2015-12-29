statspec totalUpdateEvents * cacheperf.CachePerfStats * updateEvents
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;
statspec totalUpdateLatency * cacheperf.CachePerfStats * updateLatency
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;
expr updateLatency = totalUpdateLatency / totalUpdateEvents ops=max-min?
;
