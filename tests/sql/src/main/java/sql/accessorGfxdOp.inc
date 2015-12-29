INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $JTESTS;
 
THREADGROUP locator
  totalThreads = fcn  ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  totalVMs     = fcn "(${locatorHosts} * ${locatorVMsPerHost})" ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"locator\",
                                ${locatorHosts}, true)" ncf;           
THREADGROUP dataStoreThreads
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\",
                                ${${A}Hosts}, true)" ncf;

THREADGROUP accessorThreads
            totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) -1  " ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                                ${${B}Hosts}, true)" ncf;  

THREADGROUP ddlThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",
                               ${${B}Hosts}, true)" ncf;                                                                     
                                                                           
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_initializeFabricServer
  runMode = always
  threadGroups = dataStoreThreads, ddlThread, accessorThreads;
  
INITTASK taskClass = sql.SQLTest taskMethod = HydraTask_createGfxdLocatorTask
         threadGroups = locator;
         
INITTASK taskClass = sql.SQLTest taskMethod = HydraTask_startGfxdLocatorTask
         runMode = always
         threadGroups = locator;                                                            
                                                                           
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscDB
	threadGroups = accessorThreads;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscSchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_startFabricServer
	runMode = always
	threadGroups = dataStoreThreads, accessorThreads;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFESchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiskStores
		threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createUDTPriceType
	threadGroups = ddlThread;	
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createUUIDType
	threadGroups = ddlThread;	
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = hydraTask_createUDTPriceFunctions
	threadGroups = ddlThread;				

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscTables
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFETables
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForProcedures
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForSubquery
	threadGroups = ddlThread;	
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFunctionToPopulate
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createCompaniesTable
	threadGroups = ddlThread;
		
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_dropCompaniesFK
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_populateTables
	threadGroups = accessorThreads;	
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_addCompaniesFK
	threadGroups = ddlThread;		

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createIndexOnCompanies
	threadGroups = ddlThread;				
	
TASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_doDMLOp
	 threadGroups = accessorThreads, ddlThread;
			
CLOSETASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_verifyResultSets
	 threadGroups = ddlThread;

CLOSETASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_checkConstraints
	 threadGroups = ddlThread;	
	 				
sql.SQLPrms-useGfxdConfig = true;
sql.SQLPrms-hasCompanies=true;
sql.SQLPrms-companiesTableDDLExtension = "trade.companies:random";
sql.SQLPrms-numOfWorkers = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) " ncf;

hydra.gemfirexd.FabricServerPrms-hostData = false true false;
hydra.gemfirexd.FabricServerPrms-persistDD = true true false;