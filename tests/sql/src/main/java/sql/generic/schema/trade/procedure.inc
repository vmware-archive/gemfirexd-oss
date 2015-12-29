sql.generic.SQLGenericPrms-ddlOperations += "procedure" "dap" ;

sql.generic.SQLGenericPrms-procedureNames += "ShowCustomerProcedure" "TestInOutParamsProcedure" "AddInterestProcedure" "ShowBuyorders";

sql.SQLPrms-allowConcDDLDMLOps=true;
sql.SQLPrms-limitConcDDLOps=true;

	
INITTASK     taskClass   = sql.generic.SQLTestExecutor taskMethod  = HydraTask_createProcedures
	threadGroups = ddlThread;
	
TASK     taskClass   = sql.generic.SQLTestExecutor taskMethod  = HydraTask_doDDLOp
	threadGroups = accessorThreads, verifyThreads, ddlThread;;
	
TASK     taskClass   = sql.generic.SQLTestExecutor taskMethod  = HydraTask_callProcedures
	threadGroups = accessorThreads, verifyThreads, ddlThread;;			