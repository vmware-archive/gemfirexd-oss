//------------------------------------------------------------------------------
// individual statements
// set cacheperf.comparisons.gemfirexd.useCase1.UseCase1Prms-time = true
//------------------------------------------------------------------------------

statspec countChunkedMessagesPerSecondOut *outclient* UseCase1Stats * countChunkedMessages
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec countChunkedMessagesTotalOut *outclient* UseCase1Stats * countChunkedMessages
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec countChunkedMessagesTotalTimeOut *outclient* UseCase1Stats * countChunkedMessagesTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr countChunkedMessagesResponseTimeOut = countChunkedMessagesTotalTimeOut / countChunkedMessagesTotalOut ops=max-min?
;

statspec countUnprocessedChunksOnAckQueuePerSecondOut *outclient* UseCase1Stats * countUnprocessedChunksOnAckQueue
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec countUnprocessedChunksOnAckQueueTotalOut *outclient* UseCase1Stats * countUnprocessedChunksOnAckQueue
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec countUnprocessedChunksOnAckQueueTotalTimeOut *outclient* UseCase1Stats * countUnprocessedChunksOnAckQueueTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr countUnprocessedChunksOnAckQueueResponseTimeOut = countUnprocessedChunksOnAckQueueTotalTimeOut / countUnprocessedChunksOnAckQueueTotalOut ops=max-min?
;

statspec getBORawDataByPrimaryKeyPerSecondIn *inclient* UseCase1Stats * getBORawDataByPrimaryKey
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec getBORawDataByPrimaryKeyTotalIn *inclient* UseCase1Stats * getBORawDataByPrimaryKey
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
statspec getBORawDataByPrimaryKeyTotalTimeIn *inclient* UseCase1Stats * getBORawDataByPrimaryKeyTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr getBORawDataByPrimaryKeyResponseTimeIn = getBORawDataByPrimaryKeyTotalTimeIn / getBORawDataByPrimaryKeyTotalIn ops=max-min?
;

statspec getByBackOfficeTxnId2PerSecondIn *inclient* UseCase1Stats * getByBackOfficeTxnId2
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec getByBackOfficeTxnId2TotalIn *inclient* UseCase1Stats * getByBackOfficeTxnId2
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
statspec getByBackOfficeTxnId2TotalTimeIn *inclient* UseCase1Stats * getByBackOfficeTxnId2Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr getByBackOfficeTxnId2ResponseTimeIn = getByBackOfficeTxnId2TotalTimeIn / getByBackOfficeTxnId2TotalIn ops=max-min?
;

statspec getByBackOfficeTxnIdPerSecondOut *outclient* UseCase1Stats * getByBackOfficeTxnId
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec getByBackOfficeTxnIdTotalOut *outclient* UseCase1Stats * getByBackOfficeTxnId
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec getByBackOfficeTxnIdTotalTimeOut *outclient* UseCase1Stats * getByBackOfficeTxnIdTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr getByBackOfficeTxnIdResponseTimeOut = getByBackOfficeTxnIdTotalTimeOut / getByBackOfficeTxnIdTotalOut ops=max-min?
;

statspec getByFircosoftMessageIdPerSecondOut *outclient* UseCase1Stats * getByFircosoftMessageId
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec getByFircosoftMessageIdTotalOut *outclient* UseCase1Stats * getByFircosoftMessageId
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec getByFircosoftMessageIdTotalTimeOut *outclient* UseCase1Stats * getByFircosoftMessageIdTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr getByFircosoftMessageIdResponseTimeOut = getByFircosoftMessageIdTotalTimeOut / getByFircosoftMessageIdTotalOut ops=max-min?
;

statspec getFsMessageIdPerSecondOut *outclient* UseCase1Stats * getFsMessageId
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec getFsMessageIdTotalOut *outclient* UseCase1Stats * getFsMessageId
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec getFsMessageIdTotalTimeOut *outclient* UseCase1Stats * getFsMessageIdTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr getFsMessageIdResponseTimeOut = getFsMessageIdTotalTimeOut / getFsMessageIdTotalOut ops=max-min?
;

statspec getMQNameBOMapPerSecondIn *inclient* UseCase1Stats * getMQNameBOMap
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec getMQNameBOMapTotalIn *inclient* UseCase1Stats * getMQNameBOMap
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
statspec getMQNameBOMapTotalTimeIn *inclient* UseCase1Stats * getMQNameBOMapTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr getMQNameBOMapResponseTimeIn = getMQNameBOMapTotalTimeIn / getMQNameBOMapTotalIn ops=max-min?
;

statspec inboundOpsPerSecondIn *inclient* UseCase1Stats * inboundOpsCompleted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec inboundOpsTotal *inclient* UseCase1Stats * inboundOpsCompleted
filter=none combine=combineAcrossArchives ops=max-min? trimspec=inbound
;
statspec inboundOpsTotalTimeIn *inclient* UseCase1Stats * inboundOpTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr inboundOpsResponseTimeIn = inboundOpsTotalTimeIn / inboundOpsTotal ops=max-min?
;

statspec insertBORawDataPerSecondIn *inclient* UseCase1Stats * insertBORawData
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec insertBORawDataTotalIn *inclient* UseCase1Stats * insertBORawData
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
statspec insertBORawDataTotalTimeIn *inclient* UseCase1Stats * insertBORawDataTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr insertBORawDataResponseTimeIn = insertBORawDataTotalTimeIn / insertBORawDataTotalIn ops=max-min?
;

statspec insertChnHistPerSecondOut *outclient* UseCase1Stats * insertChnHist
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec insertChnHistTotalOut *outclient* UseCase1Stats * insertChnHist
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec insertChnHistTotalTimeOut *outclient* UseCase1Stats * insertChnHistTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr insertChnHistResponseTimeOut = insertChnHistTotalTimeOut / insertChnHistTotalOut ops=max-min?
;

statspec insertChnHistConstraintViolationsPerSecondOut *outclient* UseCase1Stats * insertChnHistConstraintViolation
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec insertChnHistConstraintViolationTotalOut *outclient* UseCase1Stats * insertChnHistConstraintViolation
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec insertChnHistConstraintViolationTotalTimeOut *outclient* UseCase1Stats * insertChnHistConstraintViolationTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr insertChnHistConstraintViolationResponseTimeOut = insertChnHistConstraintViolationTotalTimeOut / insertChnHistConstraintViolationTotalOut ops=max-min?
;

statspec insertChnHistQuery1PerSecondOut *outclient* UseCase1Stats * insertChnHistQuery1
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec insertChnHistQuery1TotalOut *outclient* UseCase1Stats * insertChnHistQuery1
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec insertChnHistQuery1TotalTimeOut *outclient* UseCase1Stats * insertChnHistQuery1Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr insertChnHistQuery1ResponseTimeOut = insertChnHistQuery1TotalTimeOut / insertChnHistQuery1TotalOut ops=max-min?
;

statspec insertChnHistQuery2PerSecondOut *outclient* UseCase1Stats * insertChnHistQuery2
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec insertChnHistQuery2TotalOut *outclient* UseCase1Stats * insertChnHistQuery2
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec insertChnHistQuery2TotalTimeOut *outclient* UseCase1Stats * insertChnHistQuery2Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr insertChnHistQuery2ResponseTimeOut = insertChnHistQuery2TotalTimeOut / insertChnHistQuery2TotalOut ops=max-min?
;

statspec insertExtraBOTablesPerSecondOut *outclient* UseCase1Stats * insertExtraBOTables
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec insertExtraBOTablesTotalOut *outclient* UseCase1Stats * insertExtraBOTables
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec insertExtraBOTablesTotalTimeOut *outclient* UseCase1Stats * insertExtraBOTablesTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr insertExtraBOTablesResponseTimeOut = insertExtraBOTablesTotalTimeOut / insertExtraBOTablesTotalOut ops=max-min?
;

statspec loadSectChannelDataPerSecond *etlclient* UseCase1Stats * insertSectChannelData
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=load
;
statspec loadSectChannelDataTotal *etlclient* UseCase1Stats * insertSectChannelData
filter=none combine=combineAcrossArchives ops=max-min? trimspec=load
;
statspec loadSectChannelDataTotalTime *etlclient* UseCase1Stats * insertSectChannelDataTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=load
;
expr loadSectChannelDataResponseTime = loadSectChannelDataTotalTime / loadSectChannelDataTotal ops=max-min?
;

statspec insertSectChannelDataPerSecond *etlclient* UseCase1Stats * insertSectChannelData
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=etlgen
;
statspec insertSectChannelDataTotal *etlclient* UseCase1Stats * insertSectChannelData
filter=none combine=combineAcrossArchives ops=max-min? trimspec=etlgen
;
statspec insertSectChannelDataTotalTime *etlclient* UseCase1Stats * insertSectChannelDataTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=etlgen
;
expr insertSectChannelDataResponseTime = insertSectChannelDataTotalTime / insertSectChannelDataTotal ops=max-min?
;

statspec insertStatusHistPerSecondIn *inclient* UseCase1Stats * insertStatusHist
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec insertStatusHistTotalIn *inclient* UseCase1Stats * insertStatusHist
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
statspec insertStatusHistTotalTimeIn *inclient* UseCase1Stats * insertStatusHistTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr insertStatusHistResponseTimeIn = insertStatusHistTotalTimeIn / insertStatusHistTotalIn ops=max-min?
;

statspec insertStatusHistPerSecondOut *outclient* UseCase1Stats * insertStatusHist
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec insertStatusHistTotalOut *outclient* UseCase1Stats * insertStatusHist
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec insertStatusHistTotalTimeOut *outclient* UseCase1Stats * insertStatusHistTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr insertStatusHistResponseTimeOut = insertStatusHistTotalTimeOut / insertStatusHistTotalOut ops=max-min?
;

statspec insertToBOLogTablePerSecondOut *outclient* UseCase1Stats * insertToBOLogTable
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec insertToBOLogTableTotalOut *outclient* UseCase1Stats * insertToBOLogTable
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec insertToBOLogTableTotalTimeOut *outclient* UseCase1Stats * insertToBOLogTableTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr insertToBOLogTableResponseTimeOut = insertToBOLogTableTotalTimeOut / insertToBOLogTableTotalOut ops=max-min?
;

statspec matchedRowsPerSecondIn *inclient* UseCase1Stats * totalMatchedRows
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec matchedRowsTotalIn *inclient* UseCase1Stats * totalMatchedRows
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
statspec matchedRowsTotalTimeIn *inclient* UseCase1Stats * totalMatchedRowTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr matchedRowsResponseTimeIn = matchedRowsTotalTimeIn / matchedRowsTotalIn ops=max-min?
;

statspec outboundOpsPerSecondOut *outclient* UseCase1Stats * outboundOpsCompleted
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec outboundOpsTotal *outclient* UseCase1Stats * outboundOpsCompleted
filter=none combine=combineAcrossArchives ops=max-min? trimspec=outbound
;
statspec outboundOpsTotalTimeOut *outclient* UseCase1Stats * outboundOpTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr outboundOpsResponseTimeOut = outboundOpsTotalTimeOut / outboundOpsTotal ops=max-min?
;

statspec persistChunkedMessages2PerSecondOut *outclient* UseCase1Stats * persistChunkedMessages2
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec persistChunkedMessages2TotalOut *outclient* UseCase1Stats * persistChunkedMessages2
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec persistChunkedMessages2TotalTimeOut *outclient* UseCase1Stats * persistChunkedMessages2Time
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr persistChunkedMessages2ResponseTimeOut = persistChunkedMessages2TotalTimeOut / persistChunkedMessages2TotalOut ops=max-min?
;

statspec persistChunkedMessagesPerSecondIn *inclient* UseCase1Stats * persistChunkedMessages
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec persistChunkedMessagesTotalIn *inclient* UseCase1Stats * persistChunkedMessages
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
statspec persistChunkedMessagesTotalTimeIn *inclient* UseCase1Stats * persistChunkedMessagesTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr persistChunkedMessagesResponseTimeIn = persistChunkedMessagesTotalTimeIn / persistChunkedMessagesTotalIn ops=max-min?
;

statspec persistChunkedMessagesPerSecondOut *outclient* UseCase1Stats * persistChunkedMessages
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec persistChunkedMessagesTotalOut *outclient* UseCase1Stats * persistChunkedMessages
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec persistChunkedMessagesTotalTimeOut *outclient* UseCase1Stats * persistChunkedMessagesTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr persistChunkedMessagesResponseTimeOut = persistChunkedMessagesTotalTimeOut / persistChunkedMessagesTotalOut ops=max-min?
;

statspec persistFsMessagePerSecondIn *inclient* UseCase1Stats * persistFsMessage
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=inbound
;
statspec persistFsMessageTotalIn *inclient* UseCase1Stats * persistFsMessage
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
statspec persistFsMessageTotalTimeIn *inclient* UseCase1Stats * persistFsMessageTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=inbound
;
expr persistFsMessageResponseTimeIn = persistFsMessageTotalTimeIn / persistFsMessageTotalIn ops=max-min?
;

statspec persistFsMessagePerSecondOut *outclient* UseCase1Stats * persistFsMessage
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec persistFsMessageTotalOut *outclient* UseCase1Stats * persistFsMessage
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec persistFsMessageTotalTimeOut *outclient* UseCase1Stats * persistFsMessageTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr persistFsMessageResponseTimeOut = persistFsMessageTotalTimeOut / persistFsMessageTotalOut ops=max-min?
;

statspec selectBasedOnAckStatusPerSecondOut *outclient* UseCase1Stats * selectBasedOnAckStatus
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec selectBasedOnAckStatusTotalOut *outclient* UseCase1Stats * selectBasedOnAckStatus
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec selectBasedOnAckStatusTotalTimeOut *outclient* UseCase1Stats * selectBasedOnAckStatusTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr selectBasedOnAckStatusResponseTimeOut = selectBasedOnAckStatusTotalTimeOut / selectBasedOnAckStatusTotalOut ops=max-min?
;

statspec selectBasedOnChunkIdPerSecondOut *outclient* UseCase1Stats * selectBasedOnChunkId
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec selectBasedOnChunkIdTotalOut *outclient* UseCase1Stats * selectBasedOnChunkId
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec selectBasedOnChunkIdTotalTimeOut *outclient* UseCase1Stats * selectBasedOnChunkIdTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr selectBasedOnChunkIdResponseTimeOut = selectBasedOnChunkIdTotalTimeOut / selectBasedOnChunkIdTotalOut ops=max-min?
;

statspec selectBasedOnFircMsgIdPerSecondOut *outclient* UseCase1Stats * selectBasedOnFircMsgId
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec selectBasedOnFircMsgIdTotalOut *outclient* UseCase1Stats * selectBasedOnFircMsgId
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec selectBasedOnFircMsgIdTotalTimeOut *outclient* UseCase1Stats * selectBasedOnFircMsgIdTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr selectBasedOnFircMsgIdResponseTimeOut = selectBasedOnFircMsgIdTotalTimeOut / selectBasedOnFircMsgIdTotalOut ops=max-min?
;

statspec selectBasedOnOutStatusPerSecondOut *outclient* UseCase1Stats * selectBasedOnOutStatus
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec selectBasedOnOutStatusTotalOut *outclient* UseCase1Stats * selectBasedOnOutStatus
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec selectBasedOnOutStatusTotalTimeOut *outclient* UseCase1Stats * selectBasedOnOutStatusTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr selectBasedOnOutStatusResponseTimeOut = selectBasedOnOutStatusTotalTimeOut / selectBasedOnOutStatusTotalOut ops=max-min?
;

statspec selectInitialOFACMsgOrderedPerSecondOut *outclient* UseCase1Stats * selectInitialOFACMsgOrdered
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec selectInitialOFACMsgOrderedTotalOut *outclient* UseCase1Stats * selectInitialOFACMsgOrdered
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec selectInitialOFACMsgOrderedTotalTimeOut *outclient* UseCase1Stats * selectInitialOFACMsgOrderedTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr selectInitialOFACMsgOrderedResponseTimeOut = selectInitialOFACMsgOrderedTotalTimeOut / selectInitialOFACMsgOrderedTotalOut ops=max-min?
;

statspec selectInitialOFACMsgPerSecondOut *outclient* UseCase1Stats * selectInitialOFACMsg
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=outbound
;
statspec selectInitialOFACMsgTotalOut *outclient* UseCase1Stats * selectInitialOFACMsg
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
statspec selectInitialOFACMsgTotalTimeOut *outclient* UseCase1Stats * selectInitialOFACMsgTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=outbound
;
expr selectInitialOFACMsgResponseTimeOut = selectInitialOFACMsgTotalTimeOut / selectInitialOFACMsgTotalOut ops=max-min?
;
