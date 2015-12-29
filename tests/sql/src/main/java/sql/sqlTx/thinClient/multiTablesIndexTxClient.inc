hydra.Prms-testDescription = "creating index in inittask";

INITTASK     taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest
   			 taskMethod  = HydraTask_createIndex
			 threadGroups = clientThreads, ddlThread;
	
INITTASK     taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest
			 taskMethod  = HydraTask_createIndex
			 threadGroups = clientThreads, ddlThread;	

INITTASK     taskClass   = sql.sqlTx.thinClient.SQLDistTxClientTest
			 taskMethod  = HydraTask_createIndex
			 threadGroups = clientThreads, ddlThread;	

sql.SQLPrms-createIndex = true;
sql.SQLPrms-dropIndex = true;

