//------------------------------------------------------------------------------
// loads (inserts)

statspec loadsPerSecond *client* CoreWorkloadStats * inserts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=load
;
statspec loadTotal *client* CoreWorkloadStats * inserts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=load
;
statspec loadTotalTime *client* CoreWorkloadStats * insertTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=load
;
expr loadResponseTime = loadTotalTime / loadTotal ops=max-min?
;

//------------------------------------------------------------------------------
// reads

statspec readsPerSecond *client* CoreWorkloadStats * reads
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec readTotal *client* CoreWorkloadStats * reads
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec readTotalTime *client* CoreWorkloadStats * readTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr readResponseTime = readTotalTime / readTotal ops=max-min?
;

//------------------------------------------------------------------------------
// inserts

statspec insertsPerSecond *client* CoreWorkloadStats * inserts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=workload
;
statspec insertTotal *client* CoreWorkloadStats * inserts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
statspec insertTotalTime *client* CoreWorkloadStats * insertTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=workload
;
expr insertResponseTime = insertTotalTime / insertTotal ops=max-min?
;
