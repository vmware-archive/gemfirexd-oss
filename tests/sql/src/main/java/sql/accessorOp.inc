INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparams2.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p_2.inc;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $JTESTS;
hydra.gemfirexd.GfxdHelperPrms-persistDD = fcn "hydra.TestConfigFcns.oneof(\"true\", \"false\")" ncf;

hydra.GemFirePrms-roles =
  fcn "hydra.TestConfigFcns.duplicate(\"gemfirexd.datastore\", ${storeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"gemfirexd.accessor\", ${accessorHosts})" ncf; // setup accessor & dataStore
            
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
                                                                           
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_initializeGFXD
	runMode = always
	threadGroups = dataStoreThreads, ddlThread, accessorThreads;	
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscDB
	threadGroups = accessorThreads;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscSchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiscTables
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFXDDB
	runMode = always
	threadGroups = dataStoreThreads;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFXDDBForAccessors
	runMode = always
	threadGroups = accessorThreads;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFESchemas
	threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createDiskStores
		threadGroups = ddlThread;

INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createGFETables
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForProcedures
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFuncForSubquery
	threadGroups = ddlThread;	
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_createFunctionToPopulate
	threadGroups = ddlThread;
	
INITTASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_populateTables
	threadGroups = accessorThreads;			
	
TASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_doDMLOp
	 threadGroups = accessorThreads, ddlThread;
			
CLOSETASK     taskClass   = sql.SQLTest taskMethod  = HydraTask_verifyResultSets
	 threadGroups = ddlThread;
	 				
