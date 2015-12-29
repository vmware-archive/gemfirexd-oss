statspec getsPerSecond * cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=gets
;
