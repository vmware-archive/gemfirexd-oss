//------------------------------------------------------------------------------
// main: ops
//------------------------------------------------------------------------------

statspec opsPerSecond *client* YCSBStats * ops
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=main
;
statspec totalOps *client* YCSBStats * ops
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
statspec totalOpTime *client* YCSBStats * opTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
expr opResponseTime = totalOpTime / totalOps ops=max-min?
;

