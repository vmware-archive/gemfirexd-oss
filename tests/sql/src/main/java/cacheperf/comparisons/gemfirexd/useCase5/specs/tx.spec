//------------------------------------------------------------------------------
// transaction throughput and response times
//------------------------------------------------------------------------------

statspec insertTxCompletedPerSecond *client* UseCase5Stats * insertTxCompleted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=transactions
;
statspec totalInsertTxCompleted *client* UseCase5Stats * insertTxCompleted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=transactions
;
statspec totalInsertTxTime *client* UseCase5Stats * insertTxTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=transactions
;
expr insertTxCompletedResponseTime = totalInsertTxTime / totalInsertTxCompleted ops=max-min?
;
