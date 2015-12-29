//------------------------------------------------------------------------------
// Statistics Specifications for putAll
//------------------------------------------------------------------------------

statspec putAllEntriesPerSecond * cacheperf.comparisons.putAll.PutAllPerfStats * putAllEntries
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putAllEntries
;
statspec totalPutAllEntries * cacheperf.comparisons.putAll.PutAllPerfStats * putAllEntries
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putAllEntries
;
statspec totalPutAllTime * cacheperf.comparisons.putAll.PutAllPerfStats * putAllTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putAllEntries
;
expr putAllResponseTime = totalPutAllTime / totalPutAllEntries ops=max-min?
;
