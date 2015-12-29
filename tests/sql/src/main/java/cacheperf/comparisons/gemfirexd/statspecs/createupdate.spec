//------------------------------------------------------------------------------
// create throughput and response time
//------------------------------------------------------------------------------

statspec createsPerSecond *client* QueryPerfStats * creates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=creates
;
statspec totalCreates *client* QueryPerfStats * creates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
statspec totalCreateTime *client* QueryPerfStats * createTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=creates
;
expr createResponseTime = totalCreateTime / totalCreates ops = max-min?
;

//------------------------------------------------------------------------------
// update throughput and response time
//------------------------------------------------------------------------------

statspec updatesPerSecond *client* QueryPerfStats * updates
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=updates
;
statspec totalUpdates *client* QueryPerfStats * updates
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
statspec totalUpdateTime *client* QueryPerfStats * updateTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=updates
;
expr updateResponseTime = totalUpdateTime / totalUpdates ops = max-min?
;
