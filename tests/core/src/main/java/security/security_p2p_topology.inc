//------------------------------------------------------------------------------
// WAN TOPOLOGY WITH HIERARCHICAL CACHE SITES USING SUBGROUPS NAMED LOCATOR,
// BRIDGE, AND EDGE
//------------------------------------------------------------------------------
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
//      wanSites             number of wan sites
//      locatorHostsPerSite  number of logical locator hosts per wan site
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      bridgeHostsPerSite   number of logical bridge hosts per site
//      bridgeVMsPerHost     number of bridge VMs per bridge host 
//      bridgeThreadsPerVM   number of bridge threads per bridge VM
//      edgeHostsPerSite     number of logical edge hosts per site
//      edgeVMsPerHost       number of edge VMs per edge host
//      edgeThreadsPerVM     number of edge threads per edge VM
//------------------------------------------------------------------------------
// hydra.Prms-testTopology = "WAN topology with ${wanSites} sites, where each site is a hierarchical cache topology logically divided into a subgroups named locator, bridge, and edge.  Subgroup locator has ${locatorHostsPerSite} hosts per site, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup bridge has ${bridgeHostsPerSite} hosts per site, ${bridgeVMsPerHost} VMs per host, and ${bridgeThreadsPerVM} threads per VM.  Subgroup edge has ${edgeHostsPerSite} hosts per site, ${edgeVMsPerHost} VMs per host, and ${edgeThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host per wan site in each subgroup.
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//   bridgehost_1_1 bridgehost_1_2 ... bridgehost_1_${bridgeHostsPerSite}
//   bridgehost_2_1 bridgehost_2_2 ... bridgehost_2_${bridgeHostsPerSite}
//   ...            ...      bridgehost_${wanSites}_${bridgeHostsPerSite}
//   edgehost_1_1   edgehost_1_2   ... edgehost_1_${edgeHostsPerSite}
//   edgehost_2_1   edgehost_2_2   ... edgehost_2_${edgeHostsPerSite}
//   ...            ...      edgehost_${wanSites}_${edgeHostsPerSite}
//
hydra.HostPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_locatorhost\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
    fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_locatorhost\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_bridgehost\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
    fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_bridgehost\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
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

//   bridgevm_1_1   bridgevm_1_2   ... bridgevm_1_${bridgeHostsPerSite}
//   bridgevm_2_1   bridgevm_2_2   ... bridgevm_2_${bridgeHostsPerSite}
//   ...            ...            ... bridgevm_${wanSites}_${bridgeHostsPerSite}
//
//   edgevm_1_1     edgevm_1_2     ... edgevm_1_${edgeHostsPerSite}
//   edgevm_2_1     edgevm_2_2     ... edgevm_2_${edgeHostsPerSite}
//   ...            ...            ... edgevm_${wanSites}_${edgeHostsPerSite}
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//
//   bridgehost_1_1 bridgehost_1_2 ... bridgehost_1_${bridgeHostsPerSite}
//   bridgehost_2_1 bridgehost_2_2 ... bridgehost_2_${bridgeHostsPerSite}
//   ...            ...            ... bridgehost_${wanSites}_${bridgeHostsPerSite}
//
//   edgehost_1_1   edgehost_1_2   ... edgehost_1_${edgeHostsPerSite}
//   edgehost_2_1   edgehost_2_2   ... edgehost_2_${edgeHostsPerSite}
//   ...            ...            ... edgehost_${wanSites}_${edgeHostsPerSite}
//
//   LOCATORARGS     ...     // ${locatorHostsPerSite} * ${wanSites} times
//   BRIDGEARGS     ...      // ${bridgeHostsPerSite} * ${wanSites} times
//   EDGEARGS       ...      // ${edgeHostsPerSite} * ${wanSites} times
//
hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_locatorvm\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
    fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_locatorvm\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_bridgevm\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
    fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_bridgevm\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
  ncf;
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_locatorhost\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_locatorhost\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_bridgehost\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_bridgehost\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
;
//hydra.VmPrms-extraVMArgs   =
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"LOCATORARGS\", ${locatorHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//      (\"BRIDGEARGS\", ${bridgeHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"EDGEARGS\",   ${edgeHostsPerSite} * ${wanSites}, true)"
//  ncf;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical host per subgroup per wan site
// and maps each one to the appropriate host description
// and puts the locators and bridges for each WAN site in a different
// distributed system and each edge in distributed system "loner".
//
// locatorgemfire_1_1 ...    ...       locatorgemfire_1_${locatorHostsPerSite}
// locatorgemfire_2_1 ...    ...       locatorgemfire_2_${locatorHostsPerSite}
// ...                ...    locatorgemfire_${wanSites}_${locatorHostsPerSite}
//
// bridgegemfire_1_1  ...    ...       bridgegemfire_1_${bridgeHostsPerSite}
// bridgegemfire_2_1  ...    ...       bridgegemfire_2_${bridgeHostsPerSite}
// ...                ...    bridgegemfire_${wanSites}_${bridgeHostsPerSite}
//
// edgegemfire_1_1    ...    ...       edgegemfire_1_${edgeHostsPerSite}
// edgegemfire_2_1    ...    ...       edgegemfire_2_${edgeHostsPerSite}
// ...                ...    edgegemfire_${wanSites}_${edgeHostsPerSite}
//
// locatorhost_1_1   locatorhost_1_2   ... locatorhost_1_${locatorHostsPerSite}
// locatorhost_2_1   locatorhost 2_2   ... locatorhost_2_${locatorHostsPerSite}
// ...               ...         locatorhost_${wanSites}_${locatorHostsPerSite}
//
// bridgehost_1_1    bridgehost_1_2    ... bridgehost_1_${bridgeHostsPerSite}
// bridgehost_2_1    bridgehost_2_2    ... bridgehost_2_${bridgeHostsPerSite}
// ...               ...         bridgehost_${wanSites}_${bridgeHostsPerSite}
//
// edgehost_1_1      edgehost_1_2      ... edgehost_1_${edgeHostsPerSite}
// edgehost_2_1      edgehost_2_2      ... edgehost_2_${edgeHostsPerSite}
// ...               ...         edgehost_${wanSites}_${edgeHostsPerSite}
//
// locatords_1          locatords_1    ... // ${locatorHostsPerSite} times
// locatords_2          locatords_2    ... // ${locatorHostsPerSite} times
// locatords_${wanSites} ...           ... // ${locatorHostsPerSite} times
//
// bridgeds_1           bridgeds_1     ... // ${bridgeHostsPerSite} times
// bridgeds_2           bridgeds_2     ... // ${bridgeHostsPerSite} times
// bridgeds_${wanSites} ...            ... // ${bridgeHostsPerSite} times
//
// loner                ... // ${edgeHostsPerSite} * ${wanSites} times
//
hydra.GemFirePrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_locatorgemfire\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_locatorgemfire\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_bridgegemfire\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_bridgegemfire\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
;
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_locatorhost\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
    fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_locatorhost\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_bridgehost\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_bridgehost\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
;
hydra.GemFirePrms-distributedSystem  =
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"valid_bridgeds_\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"invalid_bridgeds_\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"valid_bridgeds_\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"invalid_bridgeds_\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
;

//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup per wan site
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//
// locator_1_1       locator_1_2       ... locator_1_${locatorHostsPerSite}
// locator_2_1       locator_2_2       ... locator_2_${locatorHostsPerSite}
// ...               ...         locator_${wanSites}_${locatorHostsPerSite}
//
// bridge_1_1        bridge_1_2        ... bridge_1_${bridgeHostsPerSite}
// bridge_2_1        bridge_2_2        ... bridge_2_${bridgeHostsPerSite}
// ...               ...         bridge_${wanSites}_${bridgeHostsPerSite}
//
// edge_1_1          bridge_1_2        ... edge_1_${edgeHostsPerSite}
// edge_2_1          bridge_2_2        ... edge_2_${edgeHostsPerSite}
// ...               ...         edge_${wanSites}_${edgeHostsPerSite}
//
// locatorvm_1_1     locatorvm_1_2     ... locatorvm_1_${locatorHostsPerSite}
// locatorvm_2_1     locatorvm_2_2     ... locatorvm_2_${locatorHostsPerSite}
// ...               ...         locatorvm_${wanSites}_${locatorHostsPerSite}
//
// bridgevm_1_1      bridgevm_1_2      ... bridgevm_1_${bridgeHostsPerSite}
// bridgevm_2_1      bridgevm_2_2      ... bridgevm_2_${bridgeHostsPerSite}
// ...               ...         bridgevm_${wanSites}_${bridgeHostsPerSite}
//
// edgevm_1_1        edgevm_1_2        ... edgevm_1_${edgeHostsPerSite}
// edgevm_2_1        edgevm_2_2        ... edgevm_2_${edgeHostsPerSite}
// ...               ...         edgevm_${wanSites}_${edgeHostsPerSite}
//
// locatorgemfire_1_1 ...    ...       locatorgemfire_1_${locatorHostsPerSite}
// locatorgemfire_2_1 ...    ...       locatorgemfire_2_${locatorHostsPerSite}
// ...                ...    locatorgemfire_${wanSites}_${locatorHostsPerSite}
//
// bridgegemfire_1_1 ...     ...       bridgegemfire_1_${bridgeHostsPerSite}
// bridgegemfire_2_1 ...     ...       bridgegemfire_2_${bridgeHostsPerSite}
// ...               ...     bridgegemfire_${wanSites}_${bridgeHostsPerSite}
//
// edgegemfire_1_1   ...     ...       edgegemfire_1_${edgeHostsPerSite}
// edgegemfire_2_1   ...     ...       edgegemfire_2_${edgeHostsPerSite}
// ...               ...     edgegemfire_${wanSites}_${edgeHostsPerSite}
//
// ${locatorThreadsPerVM}    ... ${locatorThreadsPerVM}
//                           // ${wanSites} * ${locatorHostsPerSite} times
// ${bridgeVMsPerHost}       ... 
//                           // ${bridgeHostsPerSite} * ${wanSites} times
// ${edgeVMsPerHost}         ...
//                           // ${edgeHostsPerSite} * ${wanSites} times
//
hydra.ClientPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_locator\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_locator\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_bridge\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_bridge\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_locatorvm\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_locatorvm\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_bridgevm\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_bridgevm\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
;
hydra.ClientPrms-gemfireNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_locatorgemfire\", ${validWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_locatorgemfire\", ${invalidWanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"valid_bridgegemfire\", ${validWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"invalid_bridgegemfire\", ${invalidWanSites}, ${bridgeHostsPerSite}, false)"
  ncf
;
hydra.ClientPrms-vmQuantities   =
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorVMsPerHost}\", ${validWanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorVMsPerHost}\", ${invalidWanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${bridgeVMsPerHost}\", ${bridgeHostsPerSite} * ${validWanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${bridgeVMsPerHost}\", ${bridgeHostsPerSite} * ${invalidWanSites})"
  ncf
;
hydra.ClientPrms-vmThreads   =
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorThreadsPerVM}\", ${validWanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorThreadsPerVM}\", ${invalidWanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${bridgeThreadsPerVM}\", ${bridgeHostsPerSite} * ${validWanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${bridgeThreadsPerVM}\", ${bridgeHostsPerSite} * ${invalidWanSites})"
  ncf
;

//------------------------------------------------------------------------------
// Example where all threads in the same subgroup are in the same threadgroup.
//
//THREADGROUP locator
//  totalThreads = fcn ${wanSites} * ${locatorHostsPerSite}
//                 * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP bridge
//  totalThreads = fcn ${wanSites} * ${bridgeHostsPerSite}
//                   * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"bridge\", ${wanSites}, ${bridgeHostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP edge
//  totalThreads = fcn ${wanSites} * ${edgeHostsPerSite}
//                   * ${edgeVMsPerHost} * ${edgeThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"edge\", ${wanSites}, ${edgeHostsPerSite}, false, true)"
//                 ncf;
