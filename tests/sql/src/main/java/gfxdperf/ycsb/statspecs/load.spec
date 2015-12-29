//------------------------------------------------------------------------------
// load: inserts
//------------------------------------------------------------------------------

statspec loadsPerSecond *client* YCSBStats * inserts
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=load
;
statspec loadTotalExecuted *client* YCSBStats * inserts
filter=none combine=combineAcrossArchives ops=max-min! trimspec=load
;
statspec loadTotalTime *client* YCSBStats * insertTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=load
;
expr loadResponseTime = loadTotalTime / loadTotalExecuted ops=max-min?
;
