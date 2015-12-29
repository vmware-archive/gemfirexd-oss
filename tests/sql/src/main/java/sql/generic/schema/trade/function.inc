sql.generic.SQLGenericPrms-ddlOperations += "function" ;

sql.generic.SQLGenericPrms-functionNames += "MultiplyFunction";

sql.SQLPrms-allowConcDDLDMLOps=true;
sql.SQLPrms-limitConcDDLOps=true;

	
INITTASK     taskClass   = sql.generic.SQLTestExecutor taskMethod  = HydraTask_createFunctions
	threadGroups = ddlThread;
	