//------------------------------------------------------------------------------
// WAN TOPOLOGY WITH CLIENT/SERVER CACHE SITES USING SUBGROUPS NAMED
// LOCATOR, SERVER, AND CLIENT
//------------------------------------------------------------------------------
//
// Note that it is up to the locator VMs to become gemfirexd locators through
// hydra task(s) that invoke:
//
//              hydra.gemfirexd.FabricServerHelper.createLocator
// and
//              hydra.gemfirexd.FabricServerHelper.startLocator
//
// It is important that all locators for a fabric are created before it
// or any other fabric servers connect.
//
// Note that it is up to the server VMs to become fabric servers and network
// servers through hydra task(s) that invokes:
//
//      hydra.gemfirexd.FabricServerHelper.startFabricServer
// and
//      hydra.gemfirexd.NetworkServerHelper.startNetworkServer
//
// Note that it is up to the appropriate server VMs to become gateway
// hubs through a hydra task that invokes:
//
//              hydra.GatewayHubHelper.createGatewayHub
//
// followed by a hydra task that invokes:
//
//              hydra.GatewayHubHelper.addGateways
//
// followed by a hydra task that invokes:
//
//              hydra.GatewayHubHelper.startGatewayHub
//
// It is important that all hubs are created before any hubs are started.
//
// A suitable choice of thin client connection endpoint algorithms is:
//
// hydra.gemfirexd.NetworkServerHelper.getNetworkLocatorEndpointsInWanSite
//
// or
//
// hydra.gemfirexd.NetworkServerHelper.getNetworkServerEndpointsInWanSite
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      wanSites             number of wan sites
//      locatorHostsPerSite  number of logical locator hosts per wan site
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      serverHostsPerSite   number of logical server hosts per site
//      serverVMsPerHost     number of server VMs per server host 
//      serverThreadsPerVM   number of server threads per server VM
//      clientHostsPerSite   number of logical client hosts per site
//      clientVMsPerHost     number of client VMs per client host
//      clientThreadsPerVM   number of client threads per client VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "WAN topology with ${wanSites} sites, where each site is a hierarchical cache topology logically divided into a subgroups named locator, server, and client.  Subgroup locator has ${locatorHostsPerSite} hosts per site, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup server has ${serverHostsPerSite} hosts per site, ${serverVMsPerHost} VMs per host, and ${serverThreadsPerVM} threads per VM.  Subgroup client has ${clientHostsPerSite} hosts per site, ${clientVMsPerHost} VMs per host, and ${clientThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host per wan site in each subgroup.
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...      locatorhost_${wanSites}_${locatorHostsPerSite}
//   serverhost_1_1  serverhost_1_2  ... serverhost_1_${serverHostsPerSite}
//   serverhost_2_1  serverhost_2_2  ... serverhost_2_${serverHostsPerSite}
//   ...             ...      serverhost_${wanSites}_${serverHostsPerSite}
//   clienthost_1_1  clienthost_1_2  ... clienthost_1_${clientHostsPerSite}
//   clienthost_2_1  clienthost_2_2  ... clienthost_2_${clientHostsPerSite}
//   ...             ...      clienthost_${wanSites}_${clientHostsPerSite}
//
hydra.HostPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"serverhost\", ${wanSites}, ${serverHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"clienthost\", ${wanSites}, ${clientHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNames
                            (\"controllerhost\", ${controllerHosts})"
                       ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical host per wan site in each subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//
//   locatorvm_1_1   locatorvm_1_2   ... locatorvm_1_${locatorHostsPerSite}
//   locatorvm_2_1   locatorvm_2_2   ... locatorvm_2_${locatorHostsPerSite}
//   ...             ...       locatorvm_${wanSites}_${locatorHostsPerSite}
//
//   servervm_1_1    servervm_1_2    ... servervm_1_${serverHostsPerSite}
//   servervm_2_1    servervm_2_2    ... servervm_2_${serverHostsPerSite}
//   ...             ...       servervm_${wanSites}_${serverHostsPerSite}
//
//   clientvm_1_1    clientvm_1_2    ... clientvm_1_${clientHostsPerSite}
//   clientvm_2_1    clientvm_2_2    ... clientvm_2_${clientHostsPerSite}
//   ...             ...       clientvm_${wanSites}_${clientHostsPerSite}
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//
//   serverhost_1_1  serverhost_1_2  ... serverhost_1_${serverHostsPerSite}
//   serverhost_2_1  serverhost_2_2  ... serverhost_2_${serverHostsPerSite}
//   ...             ...       serverhost_${wanSites}_${serverHostsPerSite}
//
//   clienthost_1_1  clienthost_1_2  ... clienthost_1_${clientHostsPerSite}
//   clienthost_2_1  clienthost_2_2  ... clienthost_2_${clientHostsPerSite}
//   ...             ...       clienthost_${wanSites}_${clientHostsPerSite}
//
//   locatorARGS     ...     // ${locatorHostsPerSite} * ${wanSites} times
//   serverARGS      ...     // ${serverHostsPerSite}  * ${wanSites} times
//   clientARGS      ...     // ${clientHostsPerSite}  * ${wanSites} times
//
hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorvm\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"servervm\", ${wanSites}, ${serverHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"clientvm\", ${wanSites}, ${clientHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNames
      (\"controllervm\", ${controllerHosts})"
  ncf;
  
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"serverhost\", ${wanSites}, ${serverHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"clienthost\", ${wanSites}, ${clientHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNames
     (\"controllerhost\", ${controllerHosts})"
  ncf;
//hydra.VmPrms-extraVMArgs   =
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"locatorARGS\", ${locatorHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//      (\"serverARGS\", ${serverHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"clientARGS\",   ${clientHostsPerSite} * ${wanSites}, true)"
//  ncf;

//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup per wan site
// and maps each one to the appropriate VM description
// and scales the number of VMs and threads per VM for each.
//
// locator_1_1   locator_1_2   ... locator_1_${locatorHostsPerSite}
// locator_2_1   locator_2_2   ... locator_2_${locatorHostsPerSite}
// ...           ...         locator_${wanSites}_${locatorHostsPerSite}
//
// server_1_1    server_1_2    ... server_1_${serverHostsPerSite}
// server_2_1    server_2_2    ... server_2_${serverHostsPerSite}
// ...           ...         server_${wanSites}_${serverHostsPerSite}
//
// client_1_1    client_1_2    ... client_1_${clientHostsPerSite}
// client_2_1    client_2_2    ... client_2_${clientHostsPerSite}
// ...           ...         client_${wanSites}_${clientHostsPerSite}
//
// locatorvm_1_1 locatorvm_1_2 ... locatorvm_1_${locatorHostsPerSite}
// locatorvm_2_1 locatorvm_2_2 ... locatorvm_2_${locatorHostsPerSite}
// ...           ...         locatorvm_${wanSites}_${locatorHostsPerSite}
//
// servervm_1_1  servervm_1_2  ... servervm_1_${serverHostsPerSite}
// servervm_2_1  servervm_2_2  ... servervm_2_${serverHostsPerSite}
// ...           ...         servervm_${wanSites}_${serverHostsPerSite}
//
// clientvm_1_1  clientvm_1_2  ... clientvm_1_${clientHostsPerSite}
// clientvm_2_1  clientvm_2_2  ... clientvm_2_${clientHostsPerSite}
// ...           ...         clientvm_${wanSites}_${clientHostsPerSite}
//
// ${locatorThreadsPerVM} ...  // ${wanSites} * ${locatorHostsPerSite} times
// ${serverVMsPerHost}    ...  // ${serverHostsPerSite} * ${wanSites} times
// ${clientVMsPerHost}    ...  // ${clientHostsPerSite} * ${wanSites} times
//
hydra.ClientPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"server\", ${wanSites}, ${serverHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"client\", ${wanSites}, ${clientHostsPerSite}, false)"
  ncf
   fcn "hydra.TestConfigFcns.generateNames
                                     (\"controller\", ${controllerHosts})"
                                ncf;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorvm\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"servervm\", ${wanSites}, ${serverHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"clientvm\", ${wanSites}, ${clientHostsPerSite}, false)"
  ncf
   fcn "hydra.TestConfigFcns.generateNames
                                     (\"controllervm\", ${controllerHosts})"
                                ncf;
hydra.ClientPrms-vmQuantities   =
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorVMsPerHost}\", ${wanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${serverVMsPerHost}\", ${serverHostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${clientVMsPerHost}\",   ${clientHostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
                                  (\"${controllerVMsPerHost}\", ${controllerHosts})"
                                ncf;
hydra.ClientPrms-vmThreads   =
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorThreadsPerVM}\", ${wanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${serverThreadsPerVM}\", ${serverHostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${clientThreadsPerVM}\",   ${clientHostsPerSite} * ${wanSites})"
  ncf
   fcn "hydra.TestConfigFcns.duplicate
                                  (\"${controllerThreadsPerVM}\", ${controllerHosts})"
                                ncf;

//------------------------------------------------------------------------------
// Configures a fabric server description per locator/server subgroup per wan site
// and maps each one to the appropriate client descriptions
// and puts the locators and servers for each WAN site in a different
// distributed system and configures the server subgroup to host data.
//
// locator_1   locator_2   ...   locator_${wanSites}
// server_1    server_2    ...   server_${wanSites}
//
// locator_1_1 locator_1_2 ...   locator_1_${locatorHostsPerSite},
// locator_2_1 locator 2_2 ...   locator_2_${locatorHostsPerSite},
// ...         ...         locator_${wanSites}_${locatorHostsPerSite},
//
// server_1_1  server_1_2  ...   server_1_${serverHostsPerSite},
// server_2_1  server_2_2  ...   server_2_${serverHostsPerSite},
// ...         ...         server_${wanSites}_${serverHostsPerSite},
//
// ds_1        ds_2        ...   ds_${wanSites}
// ds_1        ds_2        ...   ds_${wanSites}

hydra.gemfirexd.FabricServerPrms-names =
      fcn "hydra.TestConfigFcns.generateNames
           (\"locator_\", ${wanSites})" ncf
      fcn "hydra.TestConfigFcns.generateNames
           (\"server_\", ${wanSites})" ncf
      controller;
hydra.gemfirexd.FabricServerPrms-clientNames =
      fcn "hydra.TestConfigFcns.generateDoubleSuffixedRunsOfNames
          (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false)"
      ncf
      ,
      fcn "hydra.TestConfigFcns.generateDoubleSuffixedRunsOfNames
          (\"server\", ${wanSites}, ${serverHostsPerSite}, false)"
      ncf,
      fcn "hydra.TestConfigFcns.generateNames
           (\"controller\", ${controllerHosts})" ncf
      ;
hydra.gemfirexd.FabricServerPrms-distributedSystem  =
      fcn "hydra.TestConfigFcns.generateNames
          (\"ds_\", ${wanSites})"
      ncf
      fcn "hydra.TestConfigFcns.generateNames
          (\"ds_\", ${wanSites})"
      ncf
      ;
hydra.gemfirexd.FabricServerPrms-hostData  =
      fcn "hydra.TestConfigFcns.duplicate
          (\"false\", ${wanSites})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
          (\"true\", ${wanSites})"
      ncf
      false;

//------------------------------------------------------------------------------
// Configures a thin client description for the client subgroup
// and maps it to the appropriate client descriptions.
//
hydra.gemfirexd.ThinClientPrms-names = client;
hydra.gemfirexd.ThinClientPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames(\"client\", ${wanSites}, ${clientHostsPerSite}, false)" ncf;

//------------------------------------------------------------------------------
// Example where all threads in the same subgroup are in the same threadgroup.
//
//THREADGROUP locator
//  totalThreads = fcn ${wanSites} * ${locatorHostsPerSite}
//                 * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP server
//  totalThreads = fcn ${wanSites} * ${serverHostsPerSite}
//                   * ${serverVMsPerHost} * ${serverThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"server\", ${wanSites}, ${serverHostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP client
//  totalThreads = fcn ${wanSites} * ${clientHostsPerSite}
//                   * ${clientVMsPerHost} * ${clientThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"client\", ${wanSites}, ${clientHostsPerSite}, false, true)"
//                 ncf;
//------------------------------------------------------------------------------
