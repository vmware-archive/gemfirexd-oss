//-----------------------------------------------------------------------------------
// Statistics Specifications for Management Tests 
// 	Measures : 	gets/puts Throughput and Latency 
//				Peer/Manager CPU 
//				Network Traffic
//				Serializations Cost
//-----------------------------------------------------------------------------------

statspec getsPerSecond *bridge* cacheperf.CachePerfStats * gets
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putgets
;
statspec totalGets *bridge* cacheperf.CachePerfStats * gets
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
statspec totalGetTime *bridge* cacheperf.CachePerfStats * getTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
expr getResponseTime = totalGetTime / totalGets ops=max-min?
;

statspec putsPerSecond *bridge* cacheperf.CachePerfStats * puts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putgets
;
statspec totalPuts *bridge* cacheperf.CachePerfStats * puts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
statspec totalPutTime *bridge* cacheperf.CachePerfStats * putTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
expr putResponseTime = totalPutTime / totalPuts ops=max-min?
;

statspec memory *bridge* VMStats * totalMemory
filter=none combine=combineAcrossArchives ops=min,max,max-min? trimspec=putgets
;

statspec peercpu *bridge* LinuxSystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean? trimspec=sleep
;

statspec managercpu *locator* LinuxSystemStats * cpuActive
filter=none combine=combineAcrossArchives ops=mean? trimspec=sleep
;

//statspec memoryUse *bridge* ProcessStats * rssSize
//filter=none combine=combineAcrossArchives ops=max? trimspec=operations
//;
//statspec memoryGrowth *bridge* ProcessStats * rssSize
//filter=none combine=combineAcrossArchives ops=max-min? trimspec=operations
//;

statspec loadAverage *bridge* SystemStats * loadAverage1
filter=none combine=combineAcrossArchives ops=min,max,mean? trimspec=operations
;


statspec serztns *bridge* DistributionStats * serializations
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
statspec serztnTime *bridge* DistributionStats * serializationTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;

statspec deserztns *bridge* DistributionStats * deserializations
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;
statspec deserztnTime *bridge* DistributionStats * deserializationTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=putgets
;


statspec sentBytesPerSecond *bridge* DistributionStats * sentBytes
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putgets
;

statspec receivedBytesPerSecond *bridge* DistributionStats * receivedBytes
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=putgets
;


