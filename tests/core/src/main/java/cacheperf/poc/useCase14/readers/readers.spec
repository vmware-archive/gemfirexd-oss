// Statistics Specification for UseCase14 POC Tests
//==============================================================================

statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=mean? trimspec=gets
;
statspec totalGets * cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min trimspec=gets
;
