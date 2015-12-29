//------------------------------------------------------------------------------
// WAN TOPOLOGY WITH TWO WANS, EACH ONE WITH PEER-TO-PEER SITES 
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
//      A                        name of a wan
//      ${A}wanSites             number of wan sites for wan ${A}
//      ${A}LocatorHostsPerSite  number of logical locator hosts per wan site for wan ${A}
//      ${A}LocatorVMsPerHost    number of locator VMs per logical locator host for wan ${A}
//      ${A}LocatorThreadsPerVM  number of locator threads per locator VM for wan ${A}
//      ${A}HostsPerSite         number of logical peer hosts per wan site for wan ${A}
//      ${A}VMsPerHost           number of peer VMs per logical peer host for wan ${A}
//      ${A}ThreadsPerVM         number of peer threads per peer VM for wan ${A}
//      B                        name of another wan
//      ${B}wanSites             number of wan sites for wan ${B}
//      ${B}LocatorHostsPerSite  number of logical locator hosts per wan site for wan ${B}
//      ${B}LocatorVMsPerHost    number of locator VMs per logical locator host for wan ${B}
//      ${B}LocatorThreadsPerVM  number of locator threads per locator VM for wan ${B}
//      ${B}HostsPerSite         number of logical peer hosts per wan site for wan ${B}
//      ${B}VMsPerHost           number of peer VMs per logical peer host for wan ${B}
//      ${B}ThreadsPerVM         number of peer threads per peer VM for wan ${B}

//------------------------------------------------------------------------------
hydra.Prms-testTopology = "WAN topology with 2 wans named ${A} with ${A}wanSites sites and ${B} with ${B}wanSites, where each site is a peer-to-peer topology.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description 
hydra.HostPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}LocatorHost\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}LocatorHost\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}Host\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}Host\", ${${B}WanSites}, ${${A}HostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a VM description 
hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}LocatorVM\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}LocatorVM\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}VM\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}VM\", ${${B}WanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}LocatorHost\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}LocatorHost\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}Host\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}Host\", ${${B}WanSites}, ${${B}HostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a gemfire description 
hydra.GemFirePrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}LocatorGemFire\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}LocatorGemFire\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}GemFire\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}GemFire\", ${${B}WanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}LocatorHost\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}LocatorHost\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}Host\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}Host\", ${${B}WanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-distributedSystem  =
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"${A}DS_\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"${B}DS_\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"${A}DS_\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"${B}DS_\", ${${B}WanSites}, ${${B}HostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a client description 
hydra.ClientPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}Locator\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}Locator\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}\", ${${B}WanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}LocatorVM\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}LocatorVM\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}VM\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}VM\", ${${B}WanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.ClientPrms-gemfireNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}LocatorGemFire\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}LocatorGemFire\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}GemFire\", ${${A}WanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}GemFire\", ${${B}WanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmQuantities =
      fcn "hydra.TestConfigFcns.duplicate
           (\"${${A}LocatorVMsPerHost}\", ${${A}WanSites} * ${${A}LocatorHostsPerSite})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
           (\"${${B}LocatorVMsPerHost}\", ${${B}WanSites} * ${${B}LocatorHostsPerSite})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
           (\"${${A}VMsPerHost}\", ${${A}WanSites} * ${${A}HostsPerSite})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
           (\"${${B}VMsPerHost}\", ${${B}WanSites} * ${${B}HostsPerSite})"
      ncf;
hydra.ClientPrms-vmThreads =
      fcn "hydra.TestConfigFcns.duplicate
           (\"${${A}LocatorThreadsPerVM}\", ${${A}WanSites} * ${${A}LocatorHostsPerSite})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
           (\"${${B}LocatorThreadsPerVM}\", ${${B}WanSites} * ${${B}LocatorHostsPerSite})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
           (\"${${A}ThreadsPerVM}\", ${${A}WanSites} * ${${A}HostsPerSite})"
      ncf
      fcn "hydra.TestConfigFcns.duplicate
           (\"${${B}ThreadsPerVM}\", ${${B}WanSites} * ${${B}HostsPerSite})"
      ncf;
//------------------------------------------------------------------------------
// Example where all threads in the same subgroup are in the same threadgroup.
//
//THREADGROUP ${A}LocatorThreads
//  totalThreads = fcn ${${A}WanSites} * ${${A}LocatorHostsPerSite}
//                 * ${${A}LocatorVMsPerHost} * ${${A}LocatorThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"${A}Locator\", ${${A}WanSites}, ${${A}LocatorHostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP ${B}LocatorThreads
//  totalThreads = fcn ${${B}WanSites} * ${${B}LocatorHostsPerSite}
//                 * ${${B}LocatorVMsPerHost} * ${${B}LocatorThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"${B}Locator\", ${${B}WanSites}, ${${B}LocatorHostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP ${A}Threads
//  totalThreads = fcn ${${A}WanSites} * ${${A}HostsPerSite}
//                 * ${${A}VMsPerHost} * ${${A}ThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"${A}\", ${${A}WanSites}, ${${A}HostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP ${B}Threads
//  totalThreads = fcn ${${B}WanSites} * ${${B}HostsPerSite}
//                 * ${${B}VMsPerHost} * ${${B}ThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"${B}\", ${${B}WanSites}, ${${B}HostsPerSite}, false, true)"
//                 ncf;
//------------------------------------------------------------------------------
