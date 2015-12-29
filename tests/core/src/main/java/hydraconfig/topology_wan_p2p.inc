//------------------------------------------------------------------------------
// WAN TOPOLOGY WITH PEER-TO-PEER SITES
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
//------------------------------------------------------------------------------
// PROPERTIES:
//      wanSites          number of wan sites
//      peerHostsPerSite  number of logical peer hosts per wan site
//      peerVMsPerHost    number of peer VMs per logical peer host
//      peerThreadsPerVM  number of peer threads per peer VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "WAN topology with ${wanSites} sites, where each site is a peer-to-peer topology with ${peerHostsPerSite} hosts per site, ${peerVMsPerHost} VMs per host, and ${peerThreadsPerVM} threads per VM.";

//------------------------------------------------------------------------------
// Configures a host description per logical peer host per wan site.
//
//   host_1_1 host_1_2 ... host_1_${peerHostsPerSite}
//   host_2_1 host 2_2 ... host_2_${peerHostsPerSite}
//   ...      ...      ... host_${wanSites}_${peerHostsPerSite}
//
hydra.HostPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"host\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical peer host per wan site
// and maps each one to the appropriate host description
// and shows how to give them all the same arguments.
//
//   vm_1_1   vm_1_2   ... vm_1_${peerHostsPerSite}
//   vm_2_1   vm_2_2   ... vm_2_${peerHostsPerSite}
//   ...      ...      ... vm_${wanSites}_${peerHostsPerSite}
//
//   host_1_1 host_1_2 ... host_1_${peerHostsPerSite}
//   host_2_1 host 2_2 ... host_2_${peerHostsPerSite}
//   ...      ...      ... host_${wanSites}_${peerHostsPerSite}
//
//   ARGS
//
hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"vm\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"host\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
//hydra.VmPrms-extraVMArgs = ARGS;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical peer host per wan site
// and maps each one to the appropriate host description
// and puts each wan site in its own distributed system.
//
//   gemfire_1_1 gemfire_1_2 ... gemfire_1_${peerHostsPerSite}
//   gemfire_2_1 gemfire_2_2 ... gemfire_2_${peerHostsPerSite}
//   ...         ...         ... gemfire_${wanSites}_${peerHostsPerSite}
//
//   host_1_1    host_1_2    ... host_1_${peerHostsPerSite}
//   host_2_1    host 2_2    ... host_2_${peerHostsPerSite}
//   ...         ...         ... host_${wanSites}_${peerHostsPerSite}
//
//   ds_1        ds_1        ... ds_2        ...
//   ...         ds_${wanSites}
//
hydra.GemFirePrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"gemfire\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"host\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-distributedSystem  =
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"ds_\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a client description per logical peer host per wan site
// and maps each one to the appropriate VM description
// and maps each one to the appropriate GemFire description
// and scales the number of VMs and threads per VM for each.
//
//   client_1_1  client_1_2  ... client_1_${peerHostsPerSite}
//   client_2_1  client_2_2  ... client_2_${peerHostsPerSite}
//   ...         ...         ... client_${wanSites}_${peerHostsPerSite}
//
//   vm_1_1      vm_1_2      ... vm_1_${peerHostsPerSite}
//   vm_2_1      vm_2_2      ... vm_2_${peerHostsPerSite}
//   ...         ...         ... vm_${wanSites}_${peerHostsPerSite}
//
//   gemfire_1_1 gemfire_1_2 ... gemfire_1_${peerHostsPerSite}
//   gemfire_2_1 gemfire_2_2 ... gemfire_2_${peerHostsPerSite}
//   ...         ...         ... gemfire_${wanSites}_${peerHostsPerSite}
//
//   ${peerVMsPerHost}
//   ${peerThreadsPerVM}
//
hydra.ClientPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"client\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"vm\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-gemfireNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"gemfire\", ${wanSites}, ${peerHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmQuantities = ${peerVMsPerHost};
hydra.ClientPrms-vmThreads    = ${peerThreadsPerVM};
//------------------------------------------------------------------------------
