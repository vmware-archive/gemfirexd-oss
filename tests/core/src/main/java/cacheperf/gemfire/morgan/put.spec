statspec putsPerSecond * cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=puts
;
