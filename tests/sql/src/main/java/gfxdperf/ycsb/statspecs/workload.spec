statspec loadUsertablePerSecond * YCSBStats * usertableLoads
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=usertableLoad
;

statspec opsCompletedPerSecond *client* YCSBStats * opsCompleted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=main
;
statspec totalOpsCompleted *client* YCSBStats * opsCompleted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
statspec totalOpsTime *client* YCSBStats * opTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
expr opsCompletedResponseTime = totalOpsTime / totalOpsCompleted ops=max-min?
;

statspec readStmtsPerSecond *client* YCSBStats * readStmtsExecuted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=main
;
statspec readTotalStmtsExecuted *client* YCSBStats * readStmtsExecuted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
statspec readTotalStmtTime *client* YCSBStats * readStmtTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
expr readStmtResponseTime = readTotalStmtTime / readTotalStmtsExecuted ops=max-min?
;

statspec updateStmtsPerSecond *client* YCSBStats * updateStmtsExecuted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=main
;
statspec updateTotalStmtsExecuted *client* YCSBStats * updateStmtsExecuted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
statspec updateTotalStmtTime *client* YCSBStats * updateStmtTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
expr updateStmtResponseTime = updateTotalStmtTime / updateTotalStmtsExecuted ops=max-min?
;

statspec insertStmtsPerSecond *client* YCSBStats * insertStmtsExecuted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=main
;
statspec insertTotalStmtsExecuted *client* YCSBStats * insertStmtsExecuted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
statspec insertTotalStmtTime *client* YCSBStats * insertStmtTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=main
;
expr insertStmtResponseTime = insertTotalStmtTime / insertTotalStmtsExecuted ops=max-min?
;
