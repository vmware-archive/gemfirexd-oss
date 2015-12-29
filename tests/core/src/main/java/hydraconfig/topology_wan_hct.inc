//------------------------------------------------------------------------------
// WAN TOPOLOGY WITH HIERARCHICAL CACHE SITES
//
// Note that it is up to the appropriate client VMs to become gateway hubs
// through a hydra task that invokes:
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
// Note that it is up to the appropriate client VMs to become bridge servers
// through a hydra task that invokes:
//
//              hydra.BridgeHelper.startBridgeServer
//
// A suitable choice of edge endpoint algorithm is:
//
//              EdgeHelper.getRandomEndpointsInWanHctSite
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      wanSites            number of wan sites
//      bridgeHostsPerSite  number of logical bridge hosts per site
//      bridgeVMsPerHost    number of bridge VMs per bridge host 
//      bridgeThreadsPerVM  number of bridge threads per bridge VM
//      edgeHostsPerSite    number of logical edge hosts per site
//      edgeVMsPerHost      number of edge VMs per edge host
//      edgeThreadsPerVM    number of edge threads per edge VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "WAN topology with ${wanSites} sites, where each site is a hierarchical cache topology divided into two named subgroups, bridge and edge.  Subgroup bridge has ${bridgeHostsPerSite} hosts per site, ${bridgeVMsPerHost} VMs per host, and ${bridgeThreadsPerVM} threads per VM.  Subgroup edge has ${edgeHostsPerSite} hosts per site, ${edgeVMsPerHost} VMs per host, and ${edgeThreadsPerVM} threads per VM.";

//------------------------------------------------------------------------------
// Configures a host description per logical host per wan site in each subgroup.
//
//   bridgehost_1_1 bridgehost_1_2 ... bridgehost_1_${bridgeHostsPerSite}
//   bridgehost_2_1 bridgehost_2_2 ... bridgehost_2_${bridgeHostsPerSite}
//   ...            ...            ... bridgehost_${wanSites}_${bridgeHostsPerSite}
//
//   edgehost_1_1   edgehost_1_2   ... edgehost_1_${edgeHostsPerSite}
//   edgehost_2_1   edgehost_2_2   ... edgehost_2_${edgeHostsPerSite}
//   ...            ...            ... edgehost_${wanSites}_${edgeHostsPerSite}
//
hydra.HostPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"bridgehost\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"edgehost\", ${wanSites}, ${edgeHostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical host per wan site in each subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//
//   bridgevm_1_1   bridgevm_1_2   ... bridgevm_1_${bridgeHostsPerSite}
//   bridgevm_2_1   bridgevm_2_2   ... bridgevm_2_${bridgeHostsPerSite}
//   ...            ...            ... bridgevm_${wanSites}_${bridgeHostsPerSite}
//
//   edgevm_1_1     edgevm_1_2     ... edgevm_1_${edgeHostsPerSite}
//   edgevm_2_1     edgevm_2_2     ... edgevm_2_${edgeHostsPerSite}
//   ...            ...            ... edgevm_${wanSites}_${edgeHostsPerSite}
//
//   bridgehost_1_1 bridgehost_1_2 ... bridgehost_1_${bridgeHostsPerSite}
//   bridgehost_2_1 bridgehost_2_2 ... bridgehost_2_${bridgeHostsPerSite}
//   ...            ...            ... bridgehost_${wanSites}_${bridgeHostsPerSite}
//
//   edgehost_1_1   edgehost_1_2   ... edgehost_1_${edgeHostsPerSite}
//   edgehost_2_1   edgehost_2_2   ... edgehost_2_${edgeHostsPerSite}
//   ...            ...            ... edgehost_${wanSites}_${edgeHostsPerSite}
//
//   bridgeARGS     ...            // ${bridgeHostsPerSite} * ${wanSites} times
//   edgeARGS       ...            // ${edgeHostsPerSite} * ${wanSites} times
//
hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"bridgevm\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"edgevm\", ${wanSites}, ${edgeHostsPerSite}, false)"
  ncf;
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"bridgehost\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"edgehost\", ${wanSites}, ${edgeHostsPerSite}, false)"
  ncf;
//hydra.VmPrms-extraVMArgs   =
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"bridgeARGS\", ${bridgeHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"edgeARGS\",   ${edgeHostsPerSite} * ${wanSites}, true)"
//  ncf;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical host per subgroup per wan site
// and maps each one to the appropriate host description
// and puts the bridges in each WAN site in a different distributed system
// and each edge in distributed system "loner".
//
// bridgegemfire_1_1 bridgegemfire_1_2 ... bridgegemfire_1_${bridgeHostsPerSite}
// bridgegemfire_2_1 bridgegemfire_2_2 ... bridgegemfire_2_${bridgeHostsPerSite}
// ...               ...         bridgegemfire_${wanSites}_${bridgeHostsPerSite}
//
// edgegemfire_1_1   edgegemfire_1_2   ... edgegemfire_1_${edgeHostsPerSite}
// edgegemfire_2_1   edgegemfire_2_2   ... edgegemfire_2_${edgeHostsPerSite}
// ...               ...         edgegemfire_${wanSites}_${edgeHostsPerSite}
//
// bridgehost_1_1    bridgehost_1_2    ... bridgehost_1_${bridgeHostsPerSite}
// bridgehost_2_1    bridgehost_2_2    ... bridgehost_2_${bridgeHostsPerSite}
// ...               ...         bridgehost_${wanSites}_${bridgeHostsPerSite}
//
// edgehost_1_1      edgehost_1_2      ... edgehost_1_${edgeHostsPerSite}
// edgehost_2_1      edgehost_2_2      ... edgehost_2_${edgeHostsPerSite}
// ...               ...         edgehost_${wanSites}_${edgeHostsPerSite}
//
// bridgeds_1        bridgeds_1        ... // ${bridgeHostsPerSite} times
// bridgeds_2        bridgeds_2        ... // ${bridgeHostsPerSite} times
// bridgeds_${wanSites} ...            ... // ${bridgeHostsPerSite} times
//
// loner             ... // ${edgeHostsPerSite} * ${wanSites} times
//
hydra.GemFirePrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"bridgegemfire\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"edgegemfire\", ${wanSites}, ${edgeHostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"bridgehost\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"edgehost\", ${wanSites}, ${edgeHostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-distributedSystem  =
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"bridgeds_\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"loner\", ${wanSites} * ${edgeHostsPerSite})"
  ncf;

//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup per wan site
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//
// bridge_1_1        bridge_1_2        ... bridge_1_${bridgeHostsPerSite}
// bridge_2_1        bridge_2_2        ... bridge_2_${bridgeHostsPerSite}
// ...               ...         bridge_${wanSites}_${bridgeHostsPerSite}
//
// edge_1_1          edge_1_2          ... edge_1_${edgeHostsPerSite}
// edge_2_1          edge_2_2          ... edge_2_${edgeHostsPerSite}
// ...               ...         edge_${wanSites}_${edgeHostsPerSite}
//
// bridgevm_1_1      bridgevm_1_2      ... bridgevm_1_${bridgeHostsPerSite}
// bridgevm_2_1      bridgevm_2_2      ... bridgevm_2_${bridgeHostsPerSite}
// ...               ...         bridgevm_${wanSites}_${bridgeHostsPerSite}
//
// edgevm_1_1        edgevm_1_2        ... edgevm_1_${edgeHostsPerSite}
// edgevm_2_1        edgevm_2_2        ... edgevm_2_${edgeHostsPerSite}
// ...               ...         edgevm_${wanSites}_${edgeHostsPerSite}
//
// bridgegemfire_1_1 bridgegemfire_1_2 ... bridgegemfire_1_${bridgeHostsPerSite}
// bridgegemfire_2_1 bridgegemfire_2_2 ... bridgegemfire_2_${bridgeHostsPerSite}
// ...               ...         bridgegemfire_${wanSites}_${bridgeHostsPerSite}
//
// edgegemfire_1_1   edgegemfire_1_2   ... edgegemfire_1_${edgeHostsPerSite}
// edgegemfire_2_1   edgegemfire_2_2   ... edgegemfire_2_${edgeHostsPerSite}
// ...               ...         edgegemfire_${wanSites}_${edgeHostsPerSite}
//
// ${bridgeVMsPerHost} ...  // ${bridgeHostsPerSite} * ${wanSites} times
// ${edgeVMsPerHost}   ...  // ${edgeHostsPerSite} * ${wanSites} times
//
hydra.ClientPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"bridge\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"edge\", ${wanSites}, ${edgeHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"bridgevm\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"edgevm\", ${wanSites}, ${edgeHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-gemfireNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"bridgegemfire\", ${wanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"edgegemfire\", ${wanSites}, ${edgeHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmQuantities   =
  fcn "hydra.TestConfigFcns.duplicate
      (\"${bridgeVMsPerHost}\", ${bridgeHostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${edgeVMsPerHost}\",   ${edgeHostsPerSite} * ${wanSites})"
  ncf;
hydra.ClientPrms-vmThreads   =
  fcn "hydra.TestConfigFcns.duplicate
      (\"${bridgeThreadsPerVM}\", ${bridgeHostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${edgeThreadsPerVM}\",   ${edgeHostsPerSite} * ${wanSites})"
  ncf;

//------------------------------------------------------------------------------
// Example threadgroups where all bridge threads are in threadgroup bridge
// and all edge threads are in threadgroup edge.
//
//THREADGROUP bridge
//  totalThreads = fcn ${wanSites} * ${bridgeHostsPerSite}
//                   * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//           (\"bridge\", ${wanSites}, ${bridgeHostsPerSite}, false, true)" ncf;
//THREADGROUP edge
//  totalThreads = fcn ${wanSites} * ${edgeHostsPerSite}
//                   * ${edgeVMsPerHost} * ${edgeThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//           (\"edge\", ${wanSites}, ${edgeHostsPerSite}, false, true)" ncf;
//------------------------------------------------------------------------------
