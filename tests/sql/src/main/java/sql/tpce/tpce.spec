//------------------------------------------------------------------------------
// transaction throughput and response times
//------------------------------------------------------------------------------

statspec tradeOrderTxnCompletedPerSecond *peer* TPCEStats * tradeOrderTxnCompleted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=tpcetxn
;
statspec totalTradeOrderTxnCompleted *peer* TPCEStats * tradeOrderTxnCompleted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=tpcetxn
;
statspec tradeOrderTxnTime *peer* TPCEStats * tradeOrderTxnTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=tpcetxn
;
expr tradeOrderTxnCompletedResponseTime = tradeOrderTxnTime / totalTradeOrderTxnCompleted ops=max-min?
;

statspec tradeResultTxnCompletedPerSecond *peer* TPCEStats * tradeResultTxnCompleted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=tpcetxn
;
statspec totalTradeResultTxnCompleted *peer* TPCEStats * tradeResultTxnCompleted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=tpcetxn
;
statspec tradeResultTxnTime *peer* TPCEStats * tradeResultTxnTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=tpcetxn
;
expr tradeResultTxnCompletedResponseTime = tradeResultTxnTime / totalTradeResultTxnCompleted ops=max-min?
;

statspec marketFeedTxnCompletedPerSecond *peer* TPCEStats * marketFeedTxnCompleted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=tpcetxn
;
statspec totalMarketFeedTxnCompleted *peer* TPCEStats * marketFeedTxnCompleted
filter=none combine=combineAcrossArchives ops=max-min! trimspec=tpcetxn
;
statspec marketFeedTxnTime *peer* TPCEStats * marketFeedTxnTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=tpcetxn
;
expr marketFeedTxnCompletedResponseTime = marketFeedTxnTime / totalMarketFeedTxnCompleted ops=max-min?
;


