//------------------------------------------------------------------------------
// HIERARCHICAL CACHE TOPOLOGY WITH SUBGROUPS NAMED LOCATOR, ${A}BRIDGE, ${B}BRIDGE AND EDGE
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
//      locatorHosts           number of logical locator hosts
//      locatorVMsPerHost      number of locator VMs per logical locator host
//      locatorThreadsPerVM    number of locator threads per locator VM
//      A                      first subgroup of bridgeServers
//      ${A}bridgeHosts        number of logical bridge hosts in group A
//      ${A}bridgeVMsPerHost   number of bridge VMs per bridge host in group A
//      ${A}bridgeThreadsPerVM number of bridge threads per bridge VM in group A
//      B                      second subgroup of bridgeServers
//      ${B}bridgeHosts        number of logical bridge hosts in group B
//      ${B}bridgeVMsPerHost   number of bridge VMs per bridge host in group B
//      ${B}bridgeThreadsPerVM number of bridge threads per bridge VM in group B
//      edgeHosts              number of logical edge hosts
//      edgeVMsPerHost         number of edge VMs per edge host
//      edgeThreadsPerVM       number of edge threads per edge VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Hierarchical cache topology divided into a named subgroups locator, bridge, and edge.  Subgroup locator has ${locatorHosts} hosts, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup ${A}bridge has ${${A}bridgeHosts} hosts, ${${A}bridgeVMsPerHost} VMs per host, and ${${A}bridgeThreadsPerVM} threads per VM.  Subgroup ${B}bridge has ${${B}bridgeHosts} hosts, ${${B}bridgeVMsPerHost} VMs per host, and ${${B}bridgeThreadsPerVM} threads per VM.  Subgroup edge has ${edgeHosts} hosts, ${edgeVMsPerHost} VMs per host, and ${edgeThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     Abridgehost1   Abridgehost2   ...  Abridgehost${bridgeHosts}
//     Bbridgehost1   Bbridgehost2   ...  Bbridgehost${bridgeHosts}
//     edgehost1     edgehost2     ...  edgehost${edgeHosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"locatorhost\", ${locatorHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${A}bridgehost\", ${${A}bridgeHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${B}bridgehost\", ${${B}bridgeHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"edgehost\", ${edgeHosts})"
                       ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     locatorvm1    locatorvm2    ...  locatorvm${locatorHosts}
//     Abridgevm1    Abridgevm2    ...  Abridgevm${bridgeHosts}
//     Bbridgevm1    Bbridgevm2    ...  Bbridgevm${bridgeHosts}
//     edgevm1       edgevm2       ...  edgevm${edgeHosts}
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     Abridgehost1  Abridgehost2  ...  Abridgehost${bridgeHosts}
//     Bbridgehost1  Bbridgehost2  ...  Bbridgehost${bridgeHosts}
//     edgehost1     edgehost2     ...  edgehost${edgeHosts}
//     LOCATORARGS   LOCATORARGS   ...  LOCATORARGS // ${locatorHosts} times
//     AbridgeARGS   AbridgeARGS   ...  AbridgeARGS // ${${A}bridgeHosts} times
//     BbridgeARGS   BbridgeARGS   ...  BbridgeARGS // ${${B}bridgeHosts} times
//     edgeARGS      edgeARGS      ...  edgeARGS   // ${edgeHosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorvm\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}bridgevm\", ${${A}bridgeHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}bridgevm\", ${${B}bridgeHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"edgevm\", ${edgeHosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorhost\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}bridgehost\", ${${A}bridgeHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}bridgehost\", ${${B}bridgeHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"edgehost\", ${edgeHosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"LOCATORARGS\", ${locatorHosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"${A}bridgeARGS\", ${${A}bridgeHosts}, true)"
//                             ncf
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"${B}bridgeARGS\", ${${B}bridgeHosts}, true)"
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
//     Abridgegemfire1  Abridgegemfire2  ...  Abridgegemfire${${A}bridgeHosts}
//     Bbridgegemfire1  Bbridgegemfire2  ...  Bbridgegemfire${${B}bridgeHosts}
//     edgegemfire1     edgegemfire2     ...  edgegemfire${edgeHosts}
//     locatorhost1     locatorhost2     ...  locatorhost${locatorHosts}
//     Abridgehost1     Abridgehost2     ...  Abridgehost${${A}bridgeHosts}
//     Bbridgehost1     Bbridgehost2     ...  Bbridgehost${${B}bridgeHosts}
//     edgehost1        edgehost2        ...  edgehost${edgeHosts}
//     bridgeds         bridgeds         ...  bridgeds
//     loner            loner            ...  loner
//
hydra.GemFirePrms-names     = fcn "hydra.TestConfigFcns.generateNames
                                   (\"locatorgemfire\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}bridgegemfire\", ${${A}bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}bridgegemfire\", ${${B}bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"edgegemfire\", ${edgeHosts})"
                              ncf;
hydra.GemFirePrms-hostNames = fcn "hydra.TestConfigFcns.generateNames
                                   (\"locatorhost\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${A}bridgehost\", ${${A}bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"${B}bridgehost\", ${${B}bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.generateNames
                                   (\"edgehost\", ${edgeHosts})"
                              ncf;
hydra.GemFirePrms-distributedSystem =
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"bridgeds\", ${locatorHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"bridgeds\", ${${A}bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"bridgeds\", ${${B}bridgeHosts})"
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
//   Abridge1        Abridge2        ...  Abridge${${A}bridgeHosts}
//   Bbridge1        Bbridge2        ...  Bbridge${${B}bridgeHosts}
//   edge1           edge2           ...  edge${edgeHosts}
//   locatorvm1      locatorvm2      ...  locatorvm${locatorHosts}
//   Abridgevm1      Abridgevm2      ...  Abridgevm${${A}bridgeHosts}
//   Bbridgevm1      Bbridgevm2      ...  Bbridgevm${${B}bridgeHosts}
//   edgevm1         edgevm2         ...  edgevm${edgeHosts}
//   locatorgemfire1 locatorgemfire2 ...  locatorgemfire${locatorHosts}
//   Abridgegemfire1 Abridgegemfire2 ...  Abridgegemfire${${A}bridgeHosts}
//   Bbridgegemfire1 Bbridgegemfire2 ...  Bbridgegemfire${${B}bridgeHosts}
//   edgegemfire1    edgegemfire2    ...  edgegemfire${edgeHosts}
//   ${locatorVMsPerHost}   ... ${locatorVMsPerHost}   // ${locatorHosts} times
//   ${${A}bridgeVMsPerHost} ... ${${A}bridgeVMsPerHost}    // ${${A}bridgeHosts} times
//   ${${B}bridgeVMsPerHost} ... ${${B}bridgeVMsPerHost}    // ${${B}bridgeHosts} times
//   ${edgeVMsPerHost}      ... ${edgeVMsPerHost}      // ${edgeHosts} times
//   ${locatorThreadsPerVM} ... ${locatorThreadsPerVM} // ${locatorHosts} times
//   ${${A}bridgeThreadsPerVM} ... ${${A}bridgeThreadsPerVM}  // ${${A}bridgeHosts} times
//   ${${B}bridgeThreadsPerVM} ... ${${B}bridgeThreadsPerVM}  // ${${B}bridgeHosts} times
//   ${edgeThreadsPerVM}    ... ${edgeThreadsPerVM}    // ${edgeHosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locator\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}bridge\", ${${A}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}bridge\", ${${B}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"edge\", ${edgeHosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorvm\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}bridgevm\", ${${A}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}bridgevm\", ${${B}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"edgevm\", ${edgeHosts})"
                                ncf;
hydra.ClientPrms-gemfireNames = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorgemfire\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}bridgegemfire\", ${${A}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}bridgegemfire\", ${${B}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"edgegemfire\", ${edgeHosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorVMsPerHost}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${A}bridgeVMsPerHost}\", ${${A}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${${B}bridgeVMsPerHost}\", ${${B}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${edgeVMsPerHost}\", ${edgeHosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorThreadsPerVM}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                    (\"${${A}bridgeThreadsPerVM}\", ${${A}bridgeHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                    (\"${${B}bridgeThreadsPerVM}\", ${${B}bridgeHosts})"
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
//THREADGROUP ${A}bridge
//  totalThreads = fcn
//                 ${${A}bridgeHosts} * ${${A}bridgeVMsPerHost} * ${${A}bridgeThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"${A}bridge\", ${${A}bridgeHosts}, true)"
//                 ncf;
//THREADGROUP ${B}bridge
//  totalThreads = fcn
//                 ${${B}bridgeHosts} * ${${B}bridgeVMsPerHost} * ${${B}bridgeThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"${B}bridge\", ${${B}bridgeHosts}, true)"
//                 ncf;
//THREADGROUP edge
//  totalThreads = fcn
//                 ${edgeHosts} * ${edgeVMsPerHost} * ${edgeThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"edge\", ${edgeHosts}, true)"
//                 ncf;
//------------------------------------------------------------------------------
