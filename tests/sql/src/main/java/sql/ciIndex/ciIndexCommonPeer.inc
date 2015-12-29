hydra.Prms-testDescription = "test case-insensitive index in = queries";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/systemparams2.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p_2.inc;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd-client.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd-tools.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/jline-1.0.jar;

hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derby.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbynet.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbyclient.jar";
hydra.VmPrms-extraClassPaths        +=   "/gcm/where/java/derby/derby-10.8.2.2/jars/insane/derbytools.jar";
hydra.VmPrms-extraClassPaths        += $JTESTS;

hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xms512m -Xmx2048m\", ${${A}Hosts}, true)"
                             ncf
                              ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xmx256m \", ${${B}Hosts}, true)"
                             ncf;

hydra.Prms-totalTaskTimeSec           = 600;
hydra.Prms-maxResultWaitSec           = 480;
hydra.Prms-maxCloseTaskResultWaitSec  = 300;
hydra.Prms-serialExecution            = false;	

hydra.gemfirexd.FabricServerPrms-persistDD = true;
                     
hydra.GemFirePrms-roles +=  fcn "hydra.TestConfigFcns.duplicate
                                  (\"gemfirexd.datastore \", ${${A}Hosts}, false)"
                             ncf 
                             
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"gemfirexd.accessor \", ${${B}Hosts}, false)"
                             ncf 
                             ;
            
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
  
INITTASK     taskClass   = sql.SQLTest 
	taskMethod  = HydraTask_initialize
	runMode = always
	threadGroups = dataStoreThreads, ddlThread, accessorThreads;	 
	
		 
INITTASK     taskClass   = sql.SQLTest 
	taskMethod  = HydraTask_createGFXDDB
	runMode = always
	threadGroups = dataStoreThreads;

INITTASK     taskClass   = sql.SQLTest 
	taskMethod  = HydraTask_createGFXDDBForAccessors
	threadGroups = accessorThreads;


INITTASK taskClass   = sql.ciIndex.CaseInsensitiveIndexTest
	taskMethod  = HydraTask_createGfxdObjectsByPeers
	threadGroups = ddlThread;
  
INITTASK taskClass   = sql.ciIndex.CaseInsensitiveIndexTest
	taskMethod  = HydraTask_populateDataByPeers
	threadGroups = ddlThread;  
 
   	            
TASK     taskClass   = sql.ciIndex.CaseInsensitiveIndexTest 
                 taskMethod  = HydraTask_queryCaseInsentiveIndexByPeers
	         threadGroups = accessorThreads, ddlThread;	
 
sql.ciIndex.CaseInsensitiveIndexPrms-ddlFilePath = "sql/ciIndex/ciIndexDDL.sql";
sql.ciIndex.CaseInsensitiveIndexPrms-ciIndexDataFiles = 
	"/s2qa/tangc/workspace/ciitest/TABLE_DATA.dat"
	;
	
RANDOMINCLUDE $JTESTS/sql/ciIndex/offHeap.inc; // uses off-heap if include is randomly chosen
