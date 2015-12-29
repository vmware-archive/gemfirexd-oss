sql.SQLPrms-allowConcDDLDMLOps=true;
sql.SQLPrms-limitConcDDLOps=true;

sql.generic.SQLGenericPrms-genericIndexOperations = 
    "create index ::indexName:: on ::tableName:: (::column1:: )",
    "create index ::indexName:: on ::tableName:: (::column1:: asc , ::column2:: desc )", 
    "create unique index ::indexName:: on ::tableName:: (::column1:: desc, ::column2:: asc , ::column3:: )",
    "create global hash  index ::indexName:: on ::tableName:: (::column1::)",
    "drop index ::indexName::" 
    ;

TASK taskClass = sql.generic.SQLTestExecutor taskMethod  = HydraTask_createIndex
	 threadGroups = accessorThreads, verifyThreads, ddlThread;