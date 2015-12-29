INITTASK taskClass = sql.generic.SQLTestExecutor taskMethod  = HydraTask_createView
	 threadGroups = ddlThread;
  
INITTASK taskClass = sql.generic.SQLTestExecutor	taskMethod  = HydraTask_populateViewTables
	threadGroups = ddlThread;  
 
TASK  taskClass = sql.generic.SQLTestExecutor taskMethod  = HydraTask_queryViews
	  threadGroups = accessorThreads, ddlThread;
	         
CLOSETASK taskClass = sql.generic.SQLTestExecutor taskMethod  = HydraTask_verifyViews
      threadGroups = ddlThread;	
    