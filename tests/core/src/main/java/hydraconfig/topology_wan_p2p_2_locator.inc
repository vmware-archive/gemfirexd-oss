//------------------------------------------------------------------------------
// WAN TOPOLOGY WITH PEER-TO-PEER SITES WITH A LOCATOR SUBGROUP AND TWO NAMED SUBGROUPS
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
//------------------------------------------------------------------------------
// PROPERTIES:
//      wanSites             number of wan sites
//      locatorHostsPerSite  number of logical locator hosts per wan site
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      A                    name of first subgroup
//      ${A}HostsPerSite     number of logical hosts per site in group A
//      ${A}VMsPerHost       number of VMs per host in group A
//      ${A}ThreadsPerVM     number of threads per VM in group A
//      B                    name of the second subgroup
//      ${B}HostsPerSite     number of logical hosts per site in group B
//      ${B}VMsPerHost       number of VMs per host in group B
//      ${B}ThreadsPerVM     number of threads per VM in group B
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "WAN topology with ${wanSites} sites, where each site is a hierarchical cache topology logically divided into a locator subgroup and two named subgroups.  Subgroup locator has ${locatorHostsPerSite} hosts per site, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup ${A} has ${${A}HostsPerSite} hosts per site, ${${A}VMsPerHost} VMs per host, and ${${A}ThreadsPerVM} threads per VM.  Subgroup ${B} has ${${B}HostsPerSite} hosts per site, ${${B}VMsPerHost} VMs per host, and ${${B}ThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host per wan site in each subgroup.
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//   Ahost_1_1       Ahost_1_2       ... Ahost_1_${${A}HostsPerSite}
//   Ahost_2_1       Ahost_2_2       ... Ahost_2_${${A}HostsPerSite}
//   ...             ...       Ahost_${wanSites}_${${A}HostsPerSite}
//   Bhost_1_1       Bhost_1_2       ... Bhost_1_${${B}HostsPerSite}
//   Bhost_2_1       Bhost_2_2       ... Bhost_2_${${B}HostsPerSite}
//   ...             ...       Bhost_${wanSites}_${${B}HostsPerSite}
//
hydra.HostPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}host\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}host\", ${wanSites}, ${${B}HostsPerSite}, false)"
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

//   Avm_1_1         Avm_1_2         ... Avm_1_${${A}HostsPerSite}
//   Avm_2_1         Avm_2_2         ... Avm_2_${${A}HostsPerSite}
//   ...             ...             ... Avm_${wanSites}_${${A}HostsPerSite}
//
//   Bvm_1_1         Bvm_1_2         ... Bvm_1_${${B}HostsPerSite}
//   Bvm_2_1         Bvm_2_2         ... Bvm_2_${${B}HostsPerSite}
//   ...             ...             ... Bvm_${wanSites}_${${B}HostsPerSite}
//
//   locatorhost_1_1 locatorhost_1_2 ... locatorhost_1_${locatorHostsPerSite}
//   locatorhost_2_1 locatorhost 2_2 ... locatorhost_2_${locatorHostsPerSite}
//   ...             ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//
//   Ahost_1_1       Ahost_1_2       ... Ahost_1_${${A}HostsPerSite}
//   Ahost_2_1       Ahost_2_2       ... Ahost_2_${${A}HostsPerSite}
//   ...             ...             ... Ahost_${wanSites}_${${A}HostsPerSite}
//
//   Bhost_1_1       Bhost_1_2       ... Bhost_1_${${B}HostsPerSite}
//   Bhost_2_1       Bhost_2_2       ... Bhost_2_${${B}HostsPerSite}
//   ...             ...             ... Bhost_${wanSites}_${${B}HostsPerSite}
//
//   LOCATORARGS     ...     // ${locatorHostsPerSite} * ${wanSites} times
//   AARGS           ...     // ${${A}HostsPerSite} * ${wanSites} times
//   BARGS           ...     // ${${B}HostsPerSite} * ${wanSites} times
//
hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorvm\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}vm\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}vm\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}host\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}host\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf;
//hydra.VmPrms-extraVMArgs   =
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"LOCATORARGS\", ${locatorHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//      (\"AARGS\", ${${A}HostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"BARGS\", ${${B}HostsPerSite} * ${wanSites}, true)"
//  ncf;

//------------------------------------------------------------------------------
// Configures a gemfire description per logical host per subgroup per wan site
// and maps each one to the appropriate host description
// and puts each each WAN site in its own distributed system.
//
// locatorgemfire_1_1 ... ... locatorgemfire_1_${locatorHostsPerSite}
// locatorgemfire_2_1 ... ... locatorgemfire_2_${locatorHostsPerSite}
// ...                ... ... locatorgemfire_${wanSites}_${locatorHostsPerSite}
//
// Agemfire_1_1       ... ...  Agemfire_1_${${A}HostsPerSite}
// Agemfire_2_1       ... ...  Agemfire_2_${${A}HostsPerSite}
// ...                ... ...  Agemfire_${wanSites}_${${A}HostsPerSite}
//
// Bgemfire_1_1       ... ...  Bgemfire_1_${${B}HostsPerSite}
// Bgemfire_2_1       ... ...  Bgemfire_2_${${B}HostsPerSite}
// ...                ... ...  Bgemfire_${wanSites}_${${B}HostsPerSite}
//
// locatorhost_1_1    locatorhost_1_2   ... locatorhost_1_${locatorHostsPerSite}
// locatorhost_2_1    locatorhost 2_2   ... locatorhost_2_${locatorHostsPerSite}
// ...                ... ...     locatorhost_${wanSites}_${locatorHostsPerSite}
//
// Ahost_1_1          Ahost_1_2    ... Ahost_1_${${A}HostsPerSite}
// Ahost_2_1          Ahost_2_2    ... Ahost_2_${${A}HostsPerSite}
// ...                ...          ... Ahost_${wanSites}_${${A}HostsPerSite}
//
// Bhost_1_1          Bhost_1_2    ... Bhost_1_${${B}HostsPerSite}
// Bhost_2_1          Bhost_2_2    ... Bhost_2_${${B}HostsPerSite}
// ...                 ...         ... Bhost_${wanSites}_${${B}HostsPerSite}
//
// locatords_1           locatords_1   ... // ${locatorHostsPerSite} times
// locatords_2           locatords_2   ... // ${locatorHostsPerSite} times
// locatords_${wanSites} ...           ... // ${locatorHostsPerSite} times
//
// Ads_1              Ads_1        ... // ${${A}HostsPerSite} times
// Ads_2              Ads_2        ... // ${${A}HostsPerSite} times
// Ads_${wanSites}    ...          ... // ${${A}HostsPerSite} times
//
// Bds_1              Bds_1        ... // ${${B}HostsPerSite} times
// Bds_2              Bds_2        ... // ${${B}HostsPerSite} times
// Bds_${wanSites}    ...          ... // ${${B}HostsPerSite} times
//
hydra.GemFirePrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorgemfire\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}gemfire\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}gemfire\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}host\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}host\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.GemFirePrms-distributedSystem  =
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"ds_\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"ds_\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly
      (\"ds_\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf;

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
// A_1_1             A_1_2             ... A_1_${${A}HostsPerSite}
// A_2_1             A_2_2             ... A_2_${${A}HostsPerSite}
// ...               ...         A_${wanSites}_${${A}HostsPerSite}
//
// B_1_1             B_1_2             ... B_1_${${B}HostsPerSite}
// B_2_1             B_2_2             ... B_2_${${B}HostsPerSite}
// ...               ...         B_${wanSites}_${${B}HostsPerSite}
//
// locatorvm_1_1     locatorvm_1_2     ... locatorvm_1_${locatorHostsPerSite}
// locatorvm_2_1     locatorvm_2_2     ... locatorvm_2_${locatorHostsPerSite}
// ...               ...         locatorvm_${wanSites}_${locatorHostsPerSite}
//
// Avm_1_1           Avm_1_2           ... Avm_1_${${A}HostsPerSite}
// Avm_2_1           Avm_2_2           ... Avm_2_${${A}HostsPerSite}
// ...               ...         Avm_${wanSites}_${${A}HostsPerSite}
//
// Bvm_1_1           Bvm_1_2           ... Bvm_1_${${B}HostsPerSite}
// Bvm_2_1           Bvm_2_2           ... Bvm_2_${${B}HostsPerSite}
// ...               ...         Bvm_${wanSites}_${${B}HostsPerSite}
//
// locatorgemfire_1_1 ...    ...       locatorgemfire_1_${locatorHostsPerSite}
// locatorgemfire_2_1 ...    ...       locatorgemfire_2_${locatorHostsPerSite}
// ...                ...    locatorgemfire_${wanSites}_${locatorHostsPerSite}
//
// Agemfire_1_1       ...    ...       Agemfire_1_${${A}HostsPerSite}
// Agemfire_2_1       ...    ...       Agemfire_2_${${A}HostsPerSite}
// ...               ...     Agemfire_${wanSites}_${${A}HostsPerSite}
//
// Bgemfire_1_1       ...    ...       Bgemfire_1_${${B}HostsPerSite}
// Bgemfire_2_1       ...    ...       Bgemfire_2_${${B}HostsPerSite}
// ...                ...    Bgemfire_${wanSites}_${${B}HostsPerSite}
//
// ${locatorThreadsPerVM}    ... ${locatorThreadsPerVM}
//                           // ${locatorHostsPerSite} * ${wanSites} times
// ${${A}VMsPerHost}         ... 
//                           // ${${A}HostsPerSite} * ${wanSites} times
// ${${B}VMsPerHost}         ...
//                           // ${${B}HostsPerSite} * ${wanSites} times
//
hydra.ClientPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorvm\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}vm\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}vm\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.ClientPrms-gemfireNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorgemfire\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}gemfire\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${B}gemfire\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmQuantities   =
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorVMsPerHost}\", ${wanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${${A}VMsPerHost}\", ${${A}HostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${${B}VMsPerHost}\",   ${${B}HostsPerSite} * ${wanSites})"
  ncf;
hydra.ClientPrms-vmThreads   =
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorThreadsPerVM}\", ${wanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${${A}ThreadsPerVM}\", ${${A}HostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${${B}ThreadsPerVM}\",   ${${B}HostsPerSite} * ${wanSites})"
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
//THREADGROUP ${A}
//  totalThreads = fcn ${wanSites} * ${${A}HostsPerSite}
//                   * ${${A}VMsPerHost} * ${${A}ThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"${A}\", ${wanSites}, ${${A}HostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP ${B}
//  totalThreads = fcn ${wanSites} * ${${B}HostsPerSite}
//                   * ${${B}VMsPerHost} * ${${B}ThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"${B}\", ${wanSites}, ${${B}HostsPerSite}, false, true)"
//                 ncf;
