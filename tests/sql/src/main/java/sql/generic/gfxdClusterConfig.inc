// 
//     GFXD Cluster configuration having locator, accessors and clients
//

include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;

// Thread groups: ThreadGroup naming guidelines
// 		locatorThreads		-> for locator threads
//  	dataStoreThreads	-> data store threads
// 		accessorsThreads	-> accessors threads excepts verifyThreads and ddlThread
// 		verifyAccessorThreads -> one thread per accessor to verify data
// 		verifyClientThreads -> one thread per client to verify data 
// 		ddlAccessorThread 	-> one thread from accessors to run DDLs
//      ddlClientThread		-> one thread from client to run DDLs

  
THREADGROUP locatorThreads
            totalThreads = fcn  ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
            totalVMs     = fcn "(${locatorHosts} * ${locatorVMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${locatorHosts}, true)" ncf;    
         
THREADGROUP dataStoreThreads
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost} *  ${${A}ThreadsPerVM}) " ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\", ${${A}Hosts}, true)" ncf;
            
THREADGROUP verifyThreads
            totalThreads = fcn "${${B}Hosts} * ${${B}VMsPerHost}" ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\",${${B}Hosts}, true)" ncf; 
                                
THREADGROUP accessorThreads
            totalThreads = fcn "(${${B}Hosts} * ${${B}VMsPerHost} *  ${${B}ThreadsPerVM}) - ((${${B}Hosts} * ${${B}VMsPerHost}) + 1)  " ncf
            totalVMs     = fcn "(${${B}Hosts} * ${${B}VMsPerHost})" ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\", ${${B}Hosts}, true)" ncf;  

THREADGROUP ddlThread
            totalThreads = 1
            totalVMs     = 1
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\", ${${B}Hosts}, true)" ncf;


// host data configuration
hydra.gemfirexd.FabricServerPrms-hostData = false true false;
hydra.gemfirexd.FabricServerPrms-persistDD = true true false;            

// memory configuration
hydra.VmPrms-extraVMArgs += "-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70";
hydra.VmPrms-extraVMArgs += fcn "hydra.TestConfigFcns.duplicate
                                (\"-Dnone\", ${locatorHosts}, true)"
                            ncf
                            ,
                            fcn "hydra.TestConfigFcns.duplicate
                                 (\"-Xms512m -Xmx1024m\", ${${A}Hosts}, true)"
                            ncf
                            ,
                            fcn "hydra.TestConfigFcns.duplicate
                                 (\"-Xmx256m\", ${${B}Hosts}, true)"
                            ncf;

// classpath configuration
hydra.VmPrms-extraClassPaths += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths += $GEMFIRE/../product-gfxd/lib/gemfirexd-tools.jar;
hydra.VmPrms-extraClassPaths += $GEMFIRE/../product-gfxd/lib/jline-1.0.jar;
                                            