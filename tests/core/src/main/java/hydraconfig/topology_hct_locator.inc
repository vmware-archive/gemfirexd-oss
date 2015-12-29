//------------------------------------------------------------------------------
// HIERARCHICAL CACHE TOPOLOGY WITH SUBGROUPS NAMED LOCATOR, BRIDGE, AND EDGE
//
// Note that it is up to the appropriate client VMs to become locators
// through a hydra task that invokes:
//
//              hydra.DistributedSystemHelper.createLocator
// and
//              hydra.DistributedSystemHelper.startLocator...
//
// It is important that all locators for a distributed system are created
// before it or any other members connect.
//
// Note that it is up to the appropriate client VMs to become bridge servers
// through a hydra task that invokes:
//
//              hydra.BridgeHelper.startBridgeServer
//
// A suitable choice of edge endpoint algorithm is:
//
//              EdgeHelper.getRandomEndpoints (default)
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      locatorHosts         number of logical locator hosts
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      bridgeHosts          number of logical bridge hosts
//      bridgeVMsPerHost     number of bridge VMs per bridge host 
//      bridgeThreadsPerVM   number of bridge threads per bridge VM
//      edgeHosts            number of logical edge hosts
//      edgeVMsPerHost       number of edge VMs per edge host
//      edgeThreadsPerVM     number of edge threads per edge VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Hierarchical cache topology divided into a named subgroups locator, bridge, and edge.  Subgroup locator has ${locatorHosts} hosts, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup bridge has ${bridgeHosts} hosts, ${bridgeVMsPerHost} VMs per host, and ${bridgeThreadsPerVM} threads per VM.  Subgroup edge has ${edgeHosts} hosts, ${edgeVMsPerHost} VMs per host, and ${edgeThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     bridgehost1   bridgehost2   ...  bridgehost${bridgeHosts}
//     edgehost1     edgehost2     ...  edgehost${edgeHosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"locatorhost\", ${locatorHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"bridgehost\", ${bridgeHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"edgehost\", ${edgeHosts})"
                       ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     locatorvm1    locatorvm2    ...  locatorvm${locatorHosts}
//     bridgevm1     bridgevm2     ...  bridgevm${bridgeHosts}
//     edgevm1       edgevm2       ...  edgevm${edgeHosts}
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     bridgehost1   bridgehost2   ...  bridgehost${bridgeHosts}
//     edgehost1     edgehost2     ...  edgehost${edgeHosts}
//     LOCATORARGS   LOCATORARGS   ...  LOCATORARGS // ${locatorHosts} times
//     bridgeARGS    bridgeARGS    ...  bridgeARGS // ${bridgeHosts} times
//     edgeARGS      edgeARGS      ...  edgeARGS   // ${edgeHosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorvm\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"bridgevm\", ${bridgeHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"edgevm\", ${edgeHosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorhost\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"bridgehost\", ${bridgeHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"edgehost\", ${edgeHosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"LOCATORARGS\", ${locatorHosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"bridgeARGS\", ${bridgeHosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"edgeARGS\", ${edgeHosts}, true)"
//                             ncf;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical host per subgroup
// and maps each one to the appropriate host description
// and puts each locator and bridge in the distributed system "bridgeds"
// and each edge in the edge subgroup in distributed system "loner".
//     locatorgemfire1  locatorgemfire2  ...  locatorgemfire${locatorHosts}
//     bridgegemfire1   bridgegemfire2   ...  bridgegemfire${bridgeHosts}
//     edgegemfire1     edgegemfire2     ...  edgegemfire${edgeHosts}
//     locatorhost1     locatorhost2     ...  locatorhost${locatorHosts}
//     bridgehost1      bridgehost2      ...  bridgehost${bridgeHosts}
//     edgehost1        edgehost2        ...  edgehost${edgeHosts}
//     bridgeds         bridgeds         ...  bridgeds
//     bridgeds         bridgeds         ...  bridgeds
//     loner            loner            ...  loner
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"locatorgemfire\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"bridgegemfire\", ${bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"edgegemfire\", ${edgeHosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"locatorhost\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"bridgehost\", ${bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"edgehost\", ${edgeHosts})"
                              ncf;
hydra.GemFirePrms-distributedSystem =
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"bridgeds\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"bridgeds\", ${bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"loner\", ${edgeHosts})"
                              ncf;

//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//   locator1        locator2        ...  locator${locatorHosts}
//   bridge1         bridge2         ...  bridge${bridgeHosts}
//   edge1           edge2           ...  edge${edgeHosts}
//   locatorvm1      locatorvm2      ...  locatorvm${locatorHosts}
//   bridgevm1       bridgevm2       ...  bridgevm${bridgeHosts}
//   edgevm1         edgevm2         ...  edgevm${edgeHosts}
//   locatorgemfire1 locatorgemfire2 ...  locatorgemfire${locatorHosts}
//   bridgegemfire1  bridgegemfire2  ...  bridgegemfire${bridgeHosts}
//   edgegemfire1    edgegemfire2    ...  edgegemfire${edgeHosts}
//   ${locatorVMsPerHost}   ... ${locatorVMsPerHost}   // ${locatorHosts} times
//   ${bridgeVMsPerHost}    ... ${bridgeVMsPerHost}    // ${bridgeHosts} times
//   ${edgeVMsPerHost}      ... ${edgeVMsPerHost}      // ${edgeHosts} times
//   ${locatorThreadsPerVM} ... ${locatorThreadsPerVM} // ${locatorHosts} times
//   ${bridgeThreadsPerVM}  ... ${bridgeThreadsPerVM}  // ${bridgeHosts} times
//   ${edgeThreadsPerVM}    ... ${edgeThreadsPerVM}    // ${edgeHosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locator\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"bridge\", ${bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"edge\", ${edgeHosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorvm\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"bridgevm\", ${bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"edgevm\", ${edgeHosts})"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorgemfire\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"bridgegemfire\", ${bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"edgegemfire\", ${edgeHosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorVMsPerHost}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${bridgeVMsPerHost}\", ${bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${edgeVMsPerHost}\", ${edgeHosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorThreadsPerVM}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                    (\"${bridgeThreadsPerVM}\", ${bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                    (\"${edgeThreadsPerVM}\", ${edgeHosts})"
                                ncf;

//------------------------------------------------------------------------------
// Example where all threads in the same subgroup are in the same threadgroup.
//
//THREADGROUP locator
//    totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
//                                       * ${locatorThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"locator\", ${locatorHosts}, true)"
//                   ncf;
//THREADGROUP bridge
//  totalThreads = fcn
//                 ${bridgeHosts} * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"bridge\", ${bridgeHosts}, true)"
//                 ncf;
//THREADGROUP edge
//  totalThreads = fcn
//                 ${edgeHosts} * ${edgeVMsPerHost} * ${edgeThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"edge\", ${edgeHosts}, true)"
//                 ncf;
//------------------------------------------------------------------------------
