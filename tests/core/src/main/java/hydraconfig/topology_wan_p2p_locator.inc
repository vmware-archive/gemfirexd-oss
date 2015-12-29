//------------------------------------------------------------------------------
// WAN TOPOLOGY WITH PEER-TO-PEER SITES USING SUBGROUPS NAMED LOCATOR AND PEER
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
// Note it is up to the appropriate client VMs to become gateway hubs
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
//------------------------------------------------------------------------------
// PROPERTIES:
//      wanSites             number of wan sites
//      locatorHostsPerSite  number of logical locator hosts per wan site
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      peerHostsPerSite     number of logical peer hosts per wan site
//      peerVMsPerHost       number of peer VMs per logical peer host
//      peerThreadsPerVM     number of peer threads per peer VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "WAN topology with ${wanSites} sites, where each site is a peer-to-peer topology logically divided into subgroups named locator and peer.  Subgroup locator has ${locatorHostsPerSite} hosts per site, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup peer has ${peerHostsPerSite} hosts per site, ${peerVMsPerHost} VMs per host, and ${peerThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host per wan site in each subgroup.
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//
//   peerhost_1_1 peerhost_1_2 ... peerhost_1_${peerHostsPerSite}
//   peerhost_2_1 peerhost 2_2 ... peerhost_2_${peerHostsPerSite}
//   ...          ...    peerhost_${wanSites}_${peerHostsPerSite}
//
hydra.HostPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"peerhost\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical host per wan site per subgroup
// and maps each one to the appropriate host description
// and shows how to give them all the same arguments.
//
//   locatorvm_1_1   locatorvm_1_2   ... locatorvm_1_${locatorHostsPerSite}
//   locatorvm_2_1   locatorvm_2_2   ... locatorvm_2_${locatorHostsPerSite}
//   ...             ...       locatorvm_${wanSites}_${locatorHostsPerSite}
//
//   peervm_1_1      peervm_1_2      ... peervm_1_${peerHostsPerSite}
//   peervm_2_1      peervm_2_2      ... peervm_2_${peerHostsPerSite}
//   ...             ...       peervm_${wanSites}_${peerHostsPerSite}
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//
//   peerhost_1_1    peerhost_1_2    ... peerhost_1_${peerHostsPerSite}
//   peerhost_2_1    peerhost 2_2    ... peerhost_2_${peerHostsPerSite}
//   ...             ...       peerhost_${wanSites}_${peerHostsPerSite}
//
//   LOCATORARGS     ...     // ${locatorHostsPerSite} * ${wanSites} times
//   PEERARGS        ...     // ${peerHostsPerSite} * ${wanSites} times
//
hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorvm\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"peervm\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"peerhost\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
//hydra.VmPrms-extraVMArgs   =
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"LOCATORARGS\", ${locatorHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"PEERARGS\",   ${peerHostsPerSite} * ${wanSites}, true)"
//  ncf;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical host per wan site per subgroup
// and maps each one to the appropriate host description
// and puts each wan site in its own distributed system.
//
//   locatorgemfire_1_1 ...    ...       locatorgemfire_1_${locatorHostsPerSite}
//   locatorgemfire_2_1 ...    ...       locatorgemfire_2_${locatorHostsPerSite}
//   ...                ...    locatorgemfire_${wanSites}_${locatorHostsPerSite}
//
//   peergemfire_1_1    ...    ...       peergemfire_1_${peerHostsPerSite}
//   peergemfire_2_1    ...    ...       peergemfire_2_${peerHostsPerSite}
//   ...                ...    peergemfire_${wanSites}_${peerHostsPerSite}
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//
//   peerhost_1_1    peerhost_1_2    ... peerhost_1_${peerHostsPerSite}
//   peerhost_2_1    peerhost 2_2    ... peerhost_2_${peerHostsPerSite}
//   ...             ...       peerhost_${wanSites}_${peerHostsPerSite}
//
//   ds_1        ds_1        ... ds_2        ...
//   ...         ds_${wanSites}  // ${locatorHostsPerSite} of each
//   ds_1        ds_1        ... ds_2        ...
//   ...         ds_${wanSites}  // ${peerHostsPerSite} of each
//
hydra.GemFirePrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorgemfire\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"peergemfire\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"peerhost\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-distributedSystem  =
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"ds_\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"ds_\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a client description per logical host per wan site per subgroup
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//
//   locator_1_1     locator_1_2     ... locator_1_${locatorHostsPerSite}
//   locator_2_1     locator_2_2     ... locator_2_${locatorHostsPerSite}
//   ...             ...       locator_${wanSites}_${locatorHostsPerSite}
//
//   peer_1_1        peer_1_2        ... peer_1_${peerHostsPerSite}
//   peer_2_1        peer_2_2        ... peer_2_${peerHostsPerSite}
//   ...             ...       peer_${wanSites}_${peerHostsPerSite}
//
//   locatorvm_1_1   locatorvm_1_2   ... locatorvm_1_${locatorHostsPerSite}
//   locatorvm_2_1   locatorvm_2_2   ... locatorvm_2_${locatorHostsPerSite}
//   ...             ...       locatorvm_${wanSites}_${locatorHostsPerSite}
//
//   peervm_1_1      peervm_1_2      ... peervm_1_${peerHostsPerSite}
//   peervm_2_1      peervm_2_2      ... peervm_2_${peerHostsPerSite}
//   ...             ...       peervm_${wanSites}_${peerHostsPerSite}
//
//   locatorgemfire_1_1 ...    ...       locatorgemfire_1_${locatorHostsPerSite}
//   locatorgemfire_2_1 ...    ...       locatorgemfire_2_${locatorHostsPerSite}
//   ...                ...    locatorgemfire_${wanSites}_${locatorHostsPerSite}
//
//   peergemfire_1_1    ...    ...       peergemfire_1_${peerHostsPerSite}
//   peergemfire_2_1    ...    ...       peergemfire_2_${peerHostsPerSite}
//   ...                ...    peergemfire_${wanSites}_${peerHostsPerSite}
//
//   ${locatorThreadsPerVM}    ... ${locatorThreadsPerVM}
//                             // ${wanSites} * ${locatorHostsPerSite} times
//   ${peerThreadsPerVM}       ... ${peerThreadsPerVM}
//                             // ${wanSites} * ${peerHostsPerSite} times
//
hydra.ClientPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"peer\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorvm\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"peervm\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-gemfireNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorgemfire\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"peergemfire\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmQuantities =
      fcn "hydra.TestConfigFcns.duplicate
           (\"${locatorVMsPerHost}\", ${wanSites} * ${locatorHostsPerSite})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
           (\"${peerVMsPerHost}\", ${wanSites} * ${peerHostsPerSite})"
      ncf;
hydra.ClientPrms-vmThreads =
      fcn "hydra.TestConfigFcns.duplicate
           (\"${locatorThreadsPerVM}\", ${wanSites} * ${locatorHostsPerSite})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
           (\"${peerThreadsPerVM}\", ${wanSites} * ${peerHostsPerSite})"
      ncf;
//------------------------------------------------------------------------------
// Example where all threads in the same subgroup are in the same threadgroup.
//
//THREADGROUP locator
//  totalThreads = fcn ${wanSites} * ${locatorHostsPerSite}
//                 * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP peer
//  totalThreads = fcn ${wanSites} * ${peerHostsPerSite}
//                 * ${peerVMsPerHost} * ${peerThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"peer\", ${wanSites}, ${peerHostsPerSite}, false, true)"
//                 ncf;
//------------------------------------------------------------------------------
