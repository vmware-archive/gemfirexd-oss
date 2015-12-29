// Statistics Specifications for Cache Performance Tests
//==============================================================================

//// CREATE KEYS
statspec createKeysPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=createKeys
;
statspec createKeyTime * cacheperf.CachePerfStats * putTime
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=createKeys
;
statspec createKeyImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=createKeys
;
statspec totalCreateKeys * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min? trimspec=createKeys
;

//// CREATES
statspec createsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creates
;
statspec createTime * cacheperf.CachePerfStats * putTime
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creates
;
statspec createImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=creates
;
statspec totalCreates * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min? trimspec=creates
;

//// PUTS
statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec totalPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min? trimspec=puts
;
statspec putTime * cacheperf.CachePerfStats * putTime
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
statspec putImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=puts
;

//// GETS
statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=gets
;
statspec totalGets * cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min? trimspec=gets
;
statspec getTime * cacheperf.CachePerfStats * getTime
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=gets
;
statspec getImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=gets
;

//// SYSTEM STATS
statspec totalImageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=default
;
statspec cpuUsed * SolarisProcessStats * cpuUsed
filter=none combine=combineAcrossArchives ops=mean? trimspec=default
;
