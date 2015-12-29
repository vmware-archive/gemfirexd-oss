statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;

statspec wanUpdateEventsPerSecond * cacheperf.CachePerfStats * updateEvents
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=puts
;

statspec totalUpdateEvents * cacheperf.CachePerfStats * updateEvents
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;

statspec totalUpdateLatency * cacheperf.CachePerfStats * updateLatency
filter=none combine=combineAcrossArchives ops=max-min! trimspec=operations
;

expr wanUpdateLatency = totalUpdateLatency / totalUpdateEvents ops=max-min?
;

statspec senderQueueSize * GatewaySenderStatistics * eventQueueSize
filter=none combine=combineAcrossArchives ops=mean? trimspec=operations
;

statspec eventsOverflownToDisk * DiskRegionStatistics * entriesOnlyOnDisk
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=operations
;


