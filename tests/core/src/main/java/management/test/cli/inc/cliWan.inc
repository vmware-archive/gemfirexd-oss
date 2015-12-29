hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar $GEMFIRE/lib/gemfire.jar $GEMFIRE/lib/gfsh-dependencies.jar;

//already applied in topology file
//hydra.VmPrms-extraVMArgs = "-Dgemfire.enableManagement=true";

THREADGROUP locatorThreads
			totalThreads = fcn 
								(${locator_VMsPerHost}*${locatorHostsPerSite}*${wanSites}) 
						   ncf 
			totalVms =     fcn 
								(${locator_VMsPerHost}*${locatorHostsPerSite}*${wanSites}) 
						   ncf  
			clientNames =  fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"locator_managing\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\", false, true)"
                           ncf                        
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
                               (\"locator_managing\", ${locatorHostsPerSite}, ${wanSites},  \"${wanSiteNames}\", false, true)"
                           ncf
                           ;
                          
THREADGROUP cliThreads 
			totalThreads = fcn 
								(${gfsh_Hosts}*${gfsh_VMsPerHost}*${gfsh_ThreadsPerVM})*${wanSites}
						   ncf 
			totalVms =     fcn 
								${gfsh_Hosts}*${gfsh_VMsPerHost}*${wanSites}
						   ncf  
			clientNames =  fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                (\"gfsh_shell\", ${gfsh_Hosts}, ${wanSites},  \"${wanSiteNames}\", false, true)"
                           ncf
                           ;                           

STARTTASK
  taskClass = management.operations.ops.CLIOperations 
  taskMethod = HydraStartTask_ReadCommandSpecs
  ;

INITTASK
  taskClass = management.test.cli.CLITest
  taskMethod = HydraInitTask_startLocator
  runMode = always
  threadGroups = locatorThreads; 
  
                             
INITTASK
  taskClass = management.test.cli.CLITest
  taskMethod = HydraInitTask_initialize
  runMode = always
  threadGroups = default, cliThreads;
  
                           
INITTASK
  taskClass = management.test.cli.CLITest
  taskMethod = HydraInitTask_becomeManager
  threadGroups = managingThreads
  runMode = always;
  
CLOSETASK
  taskClass = management.test.cli.CLITest
  taskMethod = HydraCloseTask_printFailedCommands 
  threadGroups = cliThreads 
  ;  
  
management.jmx.JMXPrms-sleepTimeFactor = 5;                                  