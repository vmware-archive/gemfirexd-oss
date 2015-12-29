
// This is just rehash of topology_4.inc. Instead of actual logical subgroups
// named of the subgroups have been set.

// PROPERTIES:

//      gfsh_Hosts         number of logical hosts in gfsh group
//      gfsh_VMsPerHost    number of VMs per host in gfsh group
//      gfsh_ThreadsPerVM  number of threads per VM in gfsh group


//      managed_1_Hosts         number of logical hosts in managed_1 group
//      managed_1_VMsPerHost    number of VMs per host in managed_1 group
//      managed_1_ThreadsPerVM  number of threads per VM in managed_1 group

//      managed_2_Hosts         number of logical hosts in managed_2 group
//      managed_2_VMsPerHost    number of VMs per host in managed_2 group
//      managed_2_ThreadsPerVM  number of threads per VM in managed_2 group

//      locator_managing_Hosts         number of logical hosts in locator_managing group
//      locator_managing_VMsPerHost    number of VMs per host in locator_managing group
//      locator_managing_ThreadsPerVM  number of threads per VM in locator_managing group

//      managing_1_Hosts         number of logical hosts in managing_1 group
//      managing_1_VMsPerHost    number of VMs per host in managing_1 group
//      managing_1_ThreadsPerVM  number of threads per VM in managing_1 group

//		locatorHostsPerSite
//		locatorThreadsPerVM
//		locator_VMsPerHost

hydra.HostPrms-names = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"gfsh_shell_host\", ${gfsh_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                       ncf
					   fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"managed_1_host\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                       ncf
                       fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"managed_2_host\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                       ncf
                       fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"locator_managing_host\", ${locator_managing_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                       ncf
                       fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"managing_1_host\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                       ncf;
                       
hydra.VmPrms-names         = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"gfsh_shell_vm\", ${gfsh_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managed_1_vm\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managed_2_vm\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"locator_managing_vm\", ${locator_managing_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managing_1_vm\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf;
                             
hydra.VmPrms-hostNames     = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"gfsh_shell_host\", ${gfsh_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managed_1_host\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managed_2_host\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"locator_managing_host\", ${locator_managing_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managing_1_host\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf;
                             
hydra.GemFirePrms-names     = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"gfsh_shell_gemfire\", ${gfsh_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managed_1_gemfire\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managed_2_gemfire\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"locator_managing_gemfire\", ${locator_managing_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managing_1_gemfire\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf;
                              
hydra.GemFirePrms-hostNames = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"gfsh_shell_host\", ${gfsh_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managed_1_host\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managed_2_host\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"locator_managing_host\", ${locator_managing_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managing_1_host\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf;
                              
hydra.GemFirePrms-distributedSystem = fcn "hydra.TestConfigFcns.duplicate
                                   (\"loner\", ${wanSites}*${gfsh_Hosts})"
                                ncf
//                                fcn "hydra.TestConfigFcns.generateNamesRepeatedly
//                                     (\"none\", ${wanSites} , ${gfsh_Hosts},true)"
//                                ncf
                                fcn "hydra.TestConfigFcns.generateNamesRepeatedly
                                     (\"ds_\", ${wanSites} , ${managed_1_Hosts},true)"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNamesRepeatedly
                                     (\"ds_\", ${wanSites}, ${managed_2_Hosts},  true)"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNamesRepeatedly
                                     (\"ds_\", ${wanSites} , ${locator_managing_Hosts},   true)"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNamesRepeatedly
                                  (\"ds_\", ${wanSites}, ${managing_1_Hosts},  true)"
                                ncf
								;								
																
                              
hydra.GemFirePrms-jmxManager =  fcn "hydra.TestConfigFcns.duplicate
                                   (\"false\", ${wanSites}*${gfsh_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites}*${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites}*${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${wanSites}*${locator_managing_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${wanSites}*${managing_1_Hosts})"
                                ncf;
                                
hydra.GemFirePrms-jmxManagerPort =  fcn "hydra.TestConfigFcns.duplicate
                                   (\"false\", ${wanSites}*${gfsh_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites}*${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites}*${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${locator_managing_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${managing_1_Hosts})"
                                ncf;
                                
hydra.GemFirePrms-jmxManagerStart = fcn "hydra.TestConfigFcns.duplicate
                                   (\"false\", ${wanSites}*${gfsh_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites}*${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites}*${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${locator_managing_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${managing_1_Hosts})"
                                ncf;                                                              
                              
hydra.ClientPrms-names        = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"gfsh_shell\", ${gfsh_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_1\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_2\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"locator_managing\", ${locator_managing_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managing_1\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"gfsh_shell_vm\", ${gfsh_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_1_vm\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_2_vm\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"locator_managing_vm\", ${locator_managing_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managing_1_vm\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"gfsh_shell_gemfire\", ${gfsh_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_1_gemfire\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_2_gemfire\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"locator_managing_gemfire\", ${locator_managing_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managing_1_gemfire\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${gfsh_VMsPerHost}\", ${wanSites} *${gfsh_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managed_1_VMsPerHost}\", ${wanSites} *${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managed_2_VMsPerHost}\", ${wanSites} *${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${locator_managing_VMsPerHost}\", ${wanSites} *${locator_managing_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managing_1_VMsPerHost}\", ${wanSites} *${managing_1_Hosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${gfsh_ThreadsPerVM}\", ${wanSites} *${gfsh_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managed_1_ThreadsPerVM}\", ${wanSites} *${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managed_2_ThreadsPerVM}\", ${wanSites} *${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${locator_managing_ThreadsPerVM}\", ${wanSites} *${locator_managing_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managing_1_ThreadsPerVM}\", ${wanSites} *${managing_1_Hosts})"
                                ncf;
                                
hydra.VmPrms-extraVMArgs =   fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=gfsh\", ${wanSites} *${gfsh_Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=managed,managed1\", ${wanSites} *${managed_1_Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=managed,managed2\", ${wanSites} *${managed_2_Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=managing,locator_managing\", ${wanSites} *${locator_managing_Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicateString
                                  (\"-Dgemfire.enableManagement=true -Dgemfire.groups=managing,managing1\", ${wanSites} *${managing_1_Hosts}, true)"
                             ncf;
                             
hydra.Prms-manageLocatorAgents=false;