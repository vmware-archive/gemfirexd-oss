// Statistics Specifications for Cache Performance Tests
//==============================================================================



//// PUTS
statspec numOfPuts * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=min,max,mean?,max-min? trimspec=puts
;
statspec maxResponseTimeForPut * cacheperf.CachePerfStats * putMaxResponseTime
filter=none combine=combineAcrossArchives ops=min,max,mean?,max-min? trimspec=puts
;
statspec PutTime  * cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=min,max,mean?,max-min? trimspec=puts
;
statspec numOfPutsOverAll * cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=min,max,mean?,max-min? trimspec=untrimmed
;
statspec maxResponseTimeForPutOverAll * cacheperf.CachePerfStats * putMaxResponseTime
filter=none combine=combineAcrossArchives ops=min,max,mean?,max-min? trimspec=untrimmed
;
statspec PutTimeOverAll  * cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=min,max,mean?,max-min? trimspec=untrimmed
;
