// Statistics Specifications for Cache Performance Tests
//==============================================================================

//// PUTS
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
statspec totalPutTime * cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=puts
;
expr putResponseTime = totalPutTime / totalPuts ops=max-min?
;
statspec putImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=puts
;
