//------------------------------------------------------------------------------
// HIERARCHICAL CACHE TOPOLOGY WITH SUBGROUPS NAMED LOCATOR, SERVER, AND CLIENT
//
// Note that it is up to the locator VMs to become gemfirexd locators through
// a hydra task that invokes:
//
//      hydra.gemfirexd.FabricServerHelper.createLocator
// and
//      hydra.gemfirexd.FabricServerHelper.startLocator
//
// It is important that all locators for a fabric are created before it
// or any other fabric servers connect.
//
// Note that it is up to the server VMs to become fabric servers and network
// servers through hydra task(s) that invoke:
//
//      hydra.gemfirexd.FabricServerHelper.startFabricServer
// and
//      hydra.gemfirexd.NetworkServerHelper.startNetworkServer
//
// A suitable choice of thin client connection endpoint algorithms is:
//
//      hydra.gemfirexd.NetworkServerHelper.getNetworkLocatorEndpoints
// or
//      hydra.gemfirexd.NetworkServerHelper.getNetworkServerEndpoints
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      locatorHosts         number of logical locator hosts
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      serverHosts          number of logical server hosts
//      serverVMsPerHost     number of server VMs per server host 
//      serverThreadsPerVM   number of server threads per server VM
//      clientHosts          number of logical client hosts
//      clientVMsPerHost     number of client VMs per client host
//      clientThreadsPerVM   number of client threads per client VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Hierarchical cache topology divided into a named subgroups locator, server, and client.  Subgroup locator has ${locatorHosts} hosts, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup server has ${serverHosts} hosts, ${serverVMsPerHost} VMs per host, and ${serverThreadsPerVM} threads per VM.  Subgroup client has ${clientHosts} hosts, ${clientVMsPerHost} VMs per host, and ${clientThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     serverhost1   serverhost2   ...  serverhost${serverHosts}
//     clienthost1   clienthost2   ...  clienthost${clientHosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"locatorhost\", ${locatorHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"serverhost\", ${serverHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"clienthost\", ${clientHosts})"
                       ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     locatorvm1    locatorvm2    ...  locatorvm${locatorHosts}
//     servervm1     servervm2     ...  servervm${serverHosts}
//     clientvm1     clientvm2     ...  clientvm${clientHosts}
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     serverhost1   serverhost2   ...  serverhost${serverHosts}
//     clienthost1   clienthost2   ...  clienthost${clientHosts}
//     locatorARGS   locatorARGS   ...  locatorARGS // ${locatorHosts} times
//     serverARGS    serverARGS    ...  serverARGS  // ${serverHosts} times
//     clientARGS    clientARGS    ...  clientARGS  // ${clientHosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorvm\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"servervm\", ${serverHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"clientvm\", ${clientHosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorhost\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"serverhost\", ${serverHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"clienthost\", ${clientHosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs = fcn "hydra.TestConfigFcns.duplicate
//                                (\"locatorARGS\", ${locatorHosts}, true)"
//                           ncf
//                           ,
//                           fcn "hydra.TestConfigFcns.duplicate
//                                (\"serverARGS\", ${serverHosts}, true)"
//                           ncf
//                           ,
//                           fcn "hydra.TestConfigFcns.duplicate
//                                (\"clientARGS\", ${clientHosts}, true)"
//                           ncf;

//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup
// and maps each one to the appropriate VM description
// and scales the number of VMs and threads per VM for each.
//   locator1        locator2        ...  locator${locatorHosts}
//   server1         server2         ...  server${serverHosts}
//   client1         client2         ...  client${clientHosts}
//   locatorvm1      locatorvm2      ...  locatorvm${locatorHosts}
//   servervm1       servervm2       ...  servervm${serverHosts}
//   clientvm1       clientvm2       ...  clientvm${clientHosts}
//   ${locatorVMsPerHost}   ... ${locatorVMsPerHost}   // ${locatorHosts} times
//   ${serverVMsPerHost}    ... ${serverVMsPerHost}    // ${serverHosts} times
//   ${clientVMsPerHost}    ... ${clientVMsPerHost}    // ${clientHosts} times
//   ${locatorThreadsPerVM} ... ${locatorThreadsPerVM} // ${locatorHosts} times
//   ${serverThreadsPerVM}  ... ${serverThreadsPerVM}  // ${serverHosts} times
//   ${clientThreadsPerVM}  ... ${clientThreadsPerVM}  // ${clientHosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locator\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"server\", ${serverHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"client\", ${clientHosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorvm\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"servervm\", ${serverHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"clientvm\", ${clientHosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorVMsPerHost}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${serverVMsPerHost}\", ${serverHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                     (\"${clientVMsPerHost}\", ${clientHosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorThreadsPerVM}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                    (\"${serverThreadsPerVM}\", ${serverHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                    (\"${clientThreadsPerVM}\", ${clientHosts})"
                                ncf;

//------------------------------------------------------------------------------
// Configures a fabric server description for the locator and bridge subgroups
// and maps each one to the appropriate client descriptions
// and puts each locator and server in the distributed system "ds"
// and configures the server subgroup to host data.
//
hydra.gemfirexd.FabricServerPrms-names = locator server;
hydra.gemfirexd.FabricServerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"locator\", ${locatorHosts})" ncf
  ,
  fcn "hydra.TestConfigFcns.generateNames(\"server\", ${serverHosts})" ncf
  ;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-hostData = false true;

//------------------------------------------------------------------------------
// Configures a thin client description for the client subgroup
// and maps it to the appropriate client descriptions.
//
hydra.gemfirexd.ThinClientPrms-names = client;
hydra.gemfirexd.ThinClientPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf;

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
//THREADGROUP server
//  totalThreads = fcn
//                 ${serverHosts} * ${serverVMsPerHost} * ${serverThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"server\", ${serverHosts}, true)"
//                 ncf;
//THREADGROUP client
//  totalThreads = fcn
//                 ${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM}
//                 ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                      (\"client\", ${clientHosts}, true)"
//                 ncf;
//------------------------------------------------------------------------------
