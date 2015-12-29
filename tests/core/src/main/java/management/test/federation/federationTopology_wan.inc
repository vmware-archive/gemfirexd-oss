// This is just rehash of topology_5.inc. Instead of actual logical subgroups
// named of the subgroups have been set.

// PROPERTIES:
//      managed_1_Hosts         number of logical hosts in managed_1 group
//      managed_1_VMsPerHost    number of VMs per host in managed_1 group
//      managed_1_ThreadsPerVM  number of threads per VM in managed_1 group

//
//      managed_2_Hosts         number of logical hosts in managed_2 group
//      managed_2_VMsPerHost    number of VMs per host in managed_2 group
//      managed_2_ThreadsPerVM  number of threads per VM in managed_2 group

//      managing_1_Hosts         number of logical hosts in managing_1 group
//      managing_1_VMsPerHost    number of VMs per host in managing_1 group
//      managing_1_ThreadsPerVM  number of threads per VM in managing_1 group

//      edge_Hosts         number of logical hosts in managing_1 group
//      edge_VMsPerHost    number of VMs per host in managing_1 group
//      edge_ThreadsPerVM  number of threads per VM in managing_1 group

//		locatorHostsPerSite
//		locatorThreadsPerVM
//		locator_VMsPerHost


hydra.HostPrms-names = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"locator_managing_host\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                       ncf
					   fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"managed_1_host\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                       ncf
                       fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"managed_2_host\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                       ncf
                       fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                            (\"managing_1_host\", ${managing_1_Hosts}, ${wanSites},\"${wanSiteNames}\", false, false)"
                       ncf
//                       fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
//                            (\"edge_host\", ${edge_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
//                       ncf
                       ;
                       
hydra.VmPrms-names         = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"locator_managing_vm\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\",  false, false)"
                             ncf
							 fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managed_1_vm\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\",  false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managed_2_vm\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\",  false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managing_1_vm\", ${managing_1_Hosts}, ${wanSites},\"${wanSiteNames}\",   false, false)"
                             ncf
//                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
//                                  (\"edge_vm\", ${edge_Hosts}, ${wanSites}, \"${wanSiteNames}\",  false, false)"
//                             ncf
                             ;
hydra.VmPrms-hostNames     = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"locator_managing_host\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\",  false, false)"
                             ncf 
							 fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managed_1_host\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managed_2_host\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"managing_1_host\",${managing_1_Hosts},${wanSites}, \"${wanSiteNames}\", false, false)"
                             ncf
//                             fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
//                                  (\"edge_host\", ${edge_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
//                             ncf
                             ;
                             
hydra.GemFirePrms-names     = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"locator_managing_gemfire\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\",  false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managed_1_gemfire\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managed_2_gemfire\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managing_1_gemfire\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
//                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
//                                  (\"edge_gemfire\", ${edge_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
//                              ncf
                              ;
hydra.GemFirePrms-hostNames = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                  (\"locator_managing_host\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\",  false, false)"
                              ncf
							  fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managed_1_host\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managed_2_host\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                   (\"managing_1_host\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                              ncf
//                              fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
//                                  (\"edge_host\", ${edge_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
//                              ncf
                              ;

hydra.GemFirePrms-distributedSystem = fcn "hydra.TestConfigFcns.generateNamesRepeatedly
                                     (\"ds_\", ${wanSites} , ${locatorHostsPerSite},true)"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNamesRepeatedly
                                     (\"ds_\", ${wanSites} , ${managed_1_Hosts},true)"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNamesRepeatedly
                                     (\"ds_\", ${wanSites}, ${managed_2_Hosts},  true)"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNamesRepeatedly
                                     (\"ds_\", ${wanSites} , ${managing_1_Hosts},   true)"
                                ncf
								;                                
//                              fcn "hydra.TestConfigFcns.generateNamesRepeatedly
//                                  (\"ds\", ${edge_Hosts}, ${wanSites},  false)"
//                              ncf
								;
								
hydra.GemFirePrms-jmxManager = fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${wanSites} * ${locatorHostsPerSite})"
                                ncf
								fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites} * ${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites} * ${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\",${wanSites} *  ${managing_1_Hosts})"
                                ncf
//                                fcn "hydra.TestConfigFcns.duplicate
//                                     (\"false\", ${wanSites} * ${edge_Hosts})"
//                                ncf
								;
								
hydra.GemFirePrms-jmxManagerPort = fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${wanSites} * ${locatorHostsPerSite})"
                                ncf
								fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites} * ${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites} * ${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\",${wanSites} *  ${managing_1_Hosts})"
                                ncf
//                                fcn "hydra.TestConfigFcns.duplicate
//                                     (\"false\", ${wanSites} * ${edge_Hosts})"
//                                ncf
								;
								
hydra.GemFirePrms-jmxManagerStart = fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\", ${wanSites} * ${locatorHostsPerSite})"
                                ncf
								fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites} * ${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"false\", ${wanSites} * ${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"true\",${wanSites} *  ${managing_1_Hosts})"
                                ncf
//                                fcn "hydra.TestConfigFcns.duplicate
//                                     (\"false\", ${wanSites} * ${edge_Hosts})"
//                                ncf
								;																				                              
                              
hydra.ClientPrms-names        = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"locator_managing_1\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
								fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_1\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_2\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managing_1\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
//                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
//                                     (\"edge\", ${edge_Hosts}, ${wanSites},  false, false)"
//                                ncf
                                ;                                
                                                                ;
hydra.ClientPrms-vmNames      = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"locator_managing_vm\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_1_vm\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_2_vm\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managing_1_vm\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
//                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
//                                     (\"edge_vm\", ${edge_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
//                                ncf
                                ;
hydra.ClientPrms-gemfireNames = fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"locator_managing_gemfire\", ${locatorHostsPerSite}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_1_gemfire\", ${managed_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managed_2_gemfire\", ${managed_2_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
                                     (\"managing_1_gemfire\", ${managing_1_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
                                ncf
//                                fcn "management.util.HydraUtil.generateNamedDoubleSuffixedNames
//                                     (\"edge_gemfire\", ${edge_Hosts}, ${wanSites}, \"${wanSiteNames}\", false, false)"
//                                ncf
                                ;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${locator_VMsPerHost}\", ${wanSites} * ${locatorHostsPerSite})"
                                ncf
								fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managed_1_VMsPerHost}\", ${wanSites} * ${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managed_2_VMsPerHost}\", ${wanSites} * ${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managing_1_VMsPerHost}\", ${wanSites} * ${managing_1_Hosts})"
                                ncf
//                                fcn "hydra.TestConfigFcns.duplicate
//                                     (\"${edge_VMsPerHost}\", ${wanSites} * ${edge_Hosts})"
//                                ncf
								;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                     (\"${locatorThreadsPerVM}\", ${wanSites} * ${locatorHostsPerSite})"
                                ncf
								fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managed_1_ThreadsPerVM}\", ${wanSites} * ${managed_1_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managed_2_ThreadsPerVM}\", ${wanSites} * ${managed_2_Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${managing_1_ThreadsPerVM}\",${wanSites} *  ${managing_1_Hosts})"
                                ncf
//                                fcn "hydra.TestConfigFcns.duplicate
//                                     (\"${edge_ThreadsPerVM}\", ${wanSites} * ${edge_Hosts})"
//                                ncf
								;                                                                                  