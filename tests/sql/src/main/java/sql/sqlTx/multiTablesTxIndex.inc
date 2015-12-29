//set table colomun info for each used by index creation, need only one thread to set in the BB
INITTASK     taskClass   = sql.sqlTx.SQLDistTxTest taskMethod  = HydraTask_setTableCols
	threadGroups = ddlThread;
 
INITTASK     taskClass   = sql.sqlTx.SQLDistTxTest taskMethod  = HydraTask_createIndex
	threadGroups = accessorThreads, ddlThread;
	
INITTASK     taskClass   = sql.sqlTx.SQLDistTxTest taskMethod  = HydraTask_createIndex
	threadGroups = accessorThreads, ddlThread;	

INITTASK     taskClass   = sql.sqlTx.SQLDistTxTest taskMethod  = HydraTask_createIndex
	threadGroups = accessorThreads, ddlThread;	

/*	to be added once lock time out is being handled and set in the test
TASK     taskClass   = sql.sqlTx.SQLDistTxTest taskMethod  = HydraTask_createIndex
	threadGroups = ddlThread;	
*/

sql.SQLPrms-createIndex = true;
sql.SQLPrms-dropIndex = ${dropIndex};