statspec createsPerSecond * cacheperf.CachePerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creates
;
statspec totalCreates * cacheperf.CachePerfStats * creates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec totalCreateTime * cacheperf.CachePerfStats * createTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
expr createResponseTime = totalCreateTime / totalCreates ops=max-min?
;

statspec imageGrowth * SolarisProcessStats * imageSize
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=creates
;
statspec cpuUsed * SolarisProcessStats * cpuUsed
filter=none combine=combineAcrossArchives ops=mean? trimspec=creates
;
