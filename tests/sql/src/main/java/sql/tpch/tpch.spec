statspec query1PerSecond * sql.tpch.TPCHStats * query1
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;

statspec totalQuery1Executions * sql.tpch.TPCHStats * query1
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

statspec totalQuery1Time * sql.tpch.TPCHStats * query1ExecutionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

expr avgQuery1ExecTimeMillis = totalQuery1Time / totalQuery1Executions ops=max-min?;




statspec query2PerSecond * sql.tpch.TPCHStats * query2
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;

statspec totalQuery2Executions * sql.tpch.TPCHStats * query2
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

statspec totalQuery2Time * sql.tpch.TPCHStats * query2ExecutionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

expr avgQuery2ExecTimeMillis = totalQuery2Time / totalQuery2Executions ops=max-min?;






statspec query3PerSecond * sql.tpch.TPCHStats * query3
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;

statspec totalQuery3Executions * sql.tpch.TPCHStats * query3
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

statspec totalQuery3Time * sql.tpch.TPCHStats * query3ExecutionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

expr avgQuery3ExecTimeMillis = totalQuery3Time / totalQuery3Executions ops=max-min?;





statspec query4PerSecond * sql.tpch.TPCHStats * query4
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;

statspec totalQuery4Executions * sql.tpch.TPCHStats * query4
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

statspec totalQuery4Time * sql.tpch.TPCHStats * query4ExecutionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

expr avgQuery4ExecTimeMillis = totalQuery4Time / totalQuery4Executions ops=max-min?;






statspec query5PerSecond * sql.tpch.TPCHStats * query5
filter=perSecond combine=combineAcrossArchives ops=min,max,mean?,stddev trimspec=queries
;

statspec totalQuery5Executions * sql.tpch.TPCHStats * query5
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

statspec totalQuery5Time * sql.tpch.TPCHStats * query5ExecutionTime
filter=none combine=combineAcrossArchives ops=max-min! trimspec=queries
;

expr avgQuery5ExecTimeMillis = totalQuery5Time / totalQuery5Executions ops=max-min?;


