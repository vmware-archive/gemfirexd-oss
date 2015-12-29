//------------------------------------------------------------------------------
// HIERARCHICAL CACHE TOPOLOGY WITH SUBGROUPS NAMED
// LOCATOR, DATATORE, AND ACCESSOR
//
// Note that it is up to the locator VMs to become gemfirexd locators through
// hydra task(s) that invoke:
//
//      hydra.gemfirexd.FabricServerHelper.createLocator
// and
//      hydra.gemfirexd.FabricServerHelper.startLocator
//
// It is important that all locators for a fabric are created before it
// or any other fabric servers connect.
//
// Note that it is up to the datastore and accessor VMs to become fabric servers
// through a hydra task that invokes:
//
//      hydra.gemfirexd.FabricServerHelper.startFabricServer
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      locatorHosts          number of logical locator hosts
//      locatorVMsPerHost     number of locator VMs per logical locator host
//      locatorThreadsPerVM   number of locator threads per locator VM
//      datastoreHosts        number of logical datastore hosts
//      datastoreVMsPerHost   number of datastore VMs per datastore host 
//      datastoreThreadsPerVM number of datastore threads per datastore VM
//      accessorHosts         number of logical accessor hosts
//      accessorVMsPerHost    number of accessor VMs per accessor host
//      accessorThreadsPerVM  number of accessor threads per accessor VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Hierarchical cache topology divided into a named subgroups locator, datastore, and accessor.  Subgroup locator has ${locatorHosts} hosts, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup datastore has ${datastoreHosts} hosts, ${datastoreVMsPerHost} VMs per host, and ${datastoreThreadsPerVM} threads per VM.  Subgroup accessor has ${accessorHosts} hosts, ${accessorVMsPerHost} VMs per host, and ${accessorThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     datastorehost1   datastorehost2   ...  datastorehost${datastoreHosts}
//     accessorhost1 accessorhost2 ...  accessorhost${accessorHosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"locatorhost\", ${locatorHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"datastorehost\", ${datastoreHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"accessorhost\", ${accessorHosts})"
                       ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     locatorvm1     locatorvm2     ...  locatorvm${locatorHosts}
//     datastorevm1   datastorevm2   ...  datastorevm${datastoreHosts}
//     accessorvm1    accessorvm2    ...  accessorvm${accessorHosts}
//     locatorhost1   locatorhost2   ...  locatorhost${locatorHosts}
//     datastorehost1 datastorehost2 ...  datastorehost${datastoreHosts}
//     accessorhost1  accessorhost2  ...  accessorhost${accessorHosts}
//     locatorARGS    locatorARGS    ...  locatorARGS // ${locatorHosts} times
//     datastoreARGS  datastoreARGS  ...  datastoreARGS // ${datastoreHosts} times
//     accessorARGS   accessorARGS   ...  accessorARGS // ${accessorHosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorvm\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"datastorevm\", ${datastoreHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"accessorvm\", ${accessorHosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorhost\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"datastorehost\", ${datastoreHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"accessorhost\", ${accessorHosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs = fcn "hydra.TestConfigFcns.duplicate
//                                (\"locatorARGS\", ${locatorHosts}, true)"
//                           ncf
//                           ,
//                           fcn "hydra.TestConfigFcns.duplicate
//                                (\"datastoreARGS\", ${datastoreHosts}, true)"
//                           ncf
//                           ,
//                           fcn "hydra.TestConfigFcns.duplicate
//                                (\"accessorARGS\", ${accessorHosts}, true)"
//                           ncf;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical host per subgroup
// and puts them all in the same distributed system.
//     locatorgemfire1  locatorgemfire2  ...  locatorgemfire${locatorHosts}
//     Agemfire1        Agemfire2        ...  Agemfire${${A}Hosts}
//     Bgemfire1        Bgemfire2        ...  Bgemfire${${B}Hosts}
//     locatorhost1     locatorhost2     ...  locatorhost${locatorHosts}
//     Ahost1           Ahost2           ...  Ahost${${A}Hosts}
//     Bhost1           Bhost2           ...  Bhost${${B}Hosts}
//     ds
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"locatorgemfire\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}gemfire\", ${${A}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}gemfire\", ${${B}Hosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"locatorhost\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}host\", ${${A}Hosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}host\", ${${B}Hosts})"
                              ncf;
hydra.GemFirePrms-distributedSystem = ds;

//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup
// and maps each one to the appropriate VM description
// and scales the number of VMs and threads per VM for each.
//  locator1        locator2        ...  locator${locatorHosts}
//  datastore1      datastore2      ...  datastore${datastoreHosts}
//  accessor1       accessor2       ...  accessor${accessorHosts}
//  locatorvm1      locatorvm2      ...  locatorvm${locatorHosts}
//  datastorevm1    datastorevm2    ...  datastorevm${datastoreHosts}
//  accessorvm1     accessorvm2     ...  accessorvm${accessorHosts}
//  ${locatorVMsPerHost}   ... ${locatorVMsPerHost}   // ${locatorHosts} times
//  ${datastoreVMsPerHost} ... ${datastoreVMsPerHost} // ${datastoreHosts} times
//  ${accessorVMsPerHost}  ... ${accessorVMsPerHost}  // ${accessorHosts} times
//  ${locatorThreadsPerVM} ... ${locatorThreadsPerVM} // ${locatorHosts} times
//  ${datastoreThreadsPerVM}.. ${datastoreThreadsPerVM} // ${datastoreHosts} times
//  ${accessorThreadsPerVM}... ${accessorThreadsPerVM}// ${accessorHosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locator\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"datastore\", ${datastoreHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"accessor\", ${accessorHosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorvm\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"datastorevm\", ${datastoreHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"accessorvm\", ${accessorHosts})"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorgemfire\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}gemfire\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}gemfire\", ${${B}Hosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorVMsPerHost}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${datastoreVMsPerHost}\", ${datastoreHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${accessorVMsPerHost}\", ${accessorHosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorThreadsPerVM}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                    (\"${datastoreThreadsPerVM}\", ${datastoreHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                    (\"${accessorThreadsPerVM}\", ${accessorHosts})"
                                ncf;

//------------------------------------------------------------------------------
// Configures a fabric server description for the named subgroups
// and maps each one to the appropriate client descriptions
// and puts each subgroup in the distributed system "ds"
// and configures the datastore subgroup to host data. 
//
hydra.gemfirexd.FabricServerPrms-names = locator datastore accessor;
hydra.gemfirexd.FabricServerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${locatorHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"datastore\", ${datastoreHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"accessor\", ${accessorHosts})" ncf
  ;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-hostData = false true false;

//------------------------------------------------------------------------------
// Example where all threads in the same subgroup are in the same threadgroup.
//
//THREADGROUP locator
//  totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
//                                     * ${locatorThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"locator\", ${locatorHosts}, true)"
//                 ncf;
//THREADGROUP datastore
//  totalThreads = fcn
//                 ${datastoreHosts} * ${datastoreVMsPerHost}
//                                   * ${datastoreThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"datastore\", ${datastoreHosts}, true)"
//                 ncf;
//THREADGROUP accessor
//  totalThreads = fcn
//                 ${accessorHosts} * ${accessorVMsPerHost}
//                                  * ${accessorThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"accessor\", ${accessorHosts}, true)"
//                 ncf;
//------------------------------------------------------------------------------
