management.jmx.JMXPrms-sleepTimeFactor = 3;
hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar $GEMFIRE/lib/gfsh-dependencies.jar;
hydra.VmPrms-extraVMArgs = "-Dgemfire.enableManagement=true";

STARTTASK
  taskClass = management.operations.ops.JMXOperations 
  taskMethod = HydraStartTask_ReadMbeanDescriptors
  ;
  
THREADGROUP managingThreads 
			totalThreads = fcn 
								(${managing_1_Hosts}*${wanSites} + ${locatorHostsPerSite}*${wanSites})  
						   ncf 
			totalVms =     fcn 
								(${managing_1_Hosts}*${wanSites} + ${locatorHostsPerSite}*${wanSites})  
						   ncf  
			clientNames =  fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                               (\"managing_1\", ${managing_1_Hosts}, ${wanSites},  \"${wanSiteNames}\", false, true)"
                           ncf
                           ,
                           fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                               (\"locator_managing_1\", ${locatorHostsPerSite}, ${wanSites},  \"${wanSiteNames}\", false, true)"
                           ncf
                           ;
THREADGROUP jmxThreads 
			totalThreads = fcn 
								(${managed_1_Hosts} + ${managed_2_Hosts})*${wanSites}  
						   ncf 
			totalVms =     fcn 
								(${managed_1_Hosts} + ${managed_2_Hosts})*${wanSites}
						   ncf  
			clientNames =  fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_1\", ${managed_1_Hosts}, ${wanSites},  \"${wanSiteNames}\", false, true)"
                           ncf,
                           fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                 (\"managed_2\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, true)"
                           ncf                     
                          ;
                                                                              

hydra.Prms-manageLocatorAgents=false;