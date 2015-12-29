//------------------------------------------------------------------------------
// WAN TOPOLOGY WITH ACCESSOR/DATASTORE CACHE SITES USING SUBGROUPS NAMED
// LOCATOR, DATASTORE, AND ACCESSOR
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
// Note that it is up to the datastore and accessor VMs to become fabric servers
// through a hydra task that invokess:
//
//      hydra.gemfirexd.FabricServerHelper.startFabricServer
//
// Note that it is up to the appropriate datastore VMs to become gateway
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
// A suitable choice of peer client connection endpoint algorithms is:
//
//------------------------------------------------------------------------------
// PROPERTIES:
//      wanSites             number of wan sites
//      locatorHostsPerSite  number of logical locator hosts per wan site
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      datastoreHostsPerSite number of logical datastore hosts per site
//      datastoreVMsPerHost   number of datastore VMs per datastore host 
//      datastoreThreadsPerVM number of datastore threads per datastore VM
//      accessorHosts        number of logical accessor hosts
//      accessorVMsPerHost   number of accessor VMs per accessor host
//      accessorThreadsPerVM number of accessor threads per accessor VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "WAN topology with ${wanSites} sites, where each site is a hierarchical cache topology logically divided into a subgroups named locator, datastore, and accessor.  Subgroup locator has ${locatorHostsPerSite} hosts per site, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup datastore has ${datastoreHostsPerSite} hosts per site, ${datastoreVMsPerHost} VMs per host, and ${datastoreThreadsPerVM} threads per VM.  Subgroup accessor has ${accessorHostsPerSite} hosts per site, ${accessorVMsPerHost} VMs per host, and ${accessorThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host per wan site in each subgroup.
//
// locatorhost_1_1   locatorhost_1_2   ... locatorhost_1_${locatorHostsPerSite}
// locatorhost_2_1   locatorhost 2_2   ... locatorhost_2_${locatorHostsPerSite}
// ...               ...      locatorhost_${wanSites}_${locatorHostsPerSite}
// datastorehost_1_1 datastorehost_1_2 ... datastorehost_1_${datastoreHostsPerSite}
// datastorehost_2_1 datastorehost_2_2 ... datastorehost_2_${datastoreHostsPerSite}
// ...               ...      datastorehost_${wanSites}_${datastoreHostsPerSite}
// accessorhost_1_1  accessorhost_1_2  ... accessorhost_1_${accessorHostsPerSite}
// accessorhost_2_1  accessorhost_2_2  ... accessorhost_2_${accessorHostsPerSite}
// ...               ...      accessorhost_${wanSites}_${accessorHostsPerSite}
//
hydra.HostPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"datastorehost\", ${wanSites}, ${datastoreHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"accessorhost\", ${wanSites}, ${accessorHostsPerSite}, false)"
  ncf;

//------------------------------------------------------------------------------
// Configures a VM description per logical host per wan site in each subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//
// locatorvm_1_1     locatorvm_1_2     ... locatorvm_1_${locatorHostsPerSite}
// locatorvm_2_1     locatorvm_2_2     ... locatorvm_2_${locatorHostsPerSite}
// ...               ...       locatorvm_${wanSites}_${locatorHostsPerSite}
//
// datastorevm_1_1   datastorevm_1_2   ... datastorevm_1_${datastoreHostsPerSite}
// datastorevm_2_1   datastorevm_2_2   ... datastorevm_2_${datastoreHostsPerSite}
// ...               ...       datastorevm_${wanSites}_${datastoreHostsPerSite}
//
// accessorvm_1_1    accessorvm_1_2    ... accessorvm_1_${accessorHostsPerSite}
// accessorvm_2_1    accessorvm_2_2    ... accessorvm_2_${accessorHostsPerSite}
// ...               ...       accessorvm_${wanSites}_${accessorHostsPerSite}
//
// locatorhost_1_1   locatorhost_1_2   ... locatorhost_1_${locatorHostsPerSite}
// locatorhost_2_1   locatorhost 2_2   ... locatorhost_2_${locatorHostsPerSite}
// ...               ...       locatorhost_${wanSites}_${locatorHostsPerSite}
//
// datastorehost_1_1 datastorehost_1_2 ... datastorehost_1_${datastoreHostsPerSite}
// datastorehost_2_1 datastorehost_2_2 ... datastorehost_2_${datastoreHostsPerSite}
// ...               ...       datastorehost_${wanSites}_${datastoreHostsPerSite}
//
// accessorhost_1_1  accessorhost_1_2 ... accessorhost_1_${accessorHostsPerSite}
// accessorhost_2_1  accessorhost_2_2 ... accessorhost_2_${accessorHostsPerSite}
// ...               ...       accessorhost_${wanSites}_${accessorHostsPerSite}
//
// locatorARGS       ...       // ${locatorHostsPerSite}   * ${wanSites} times
// datastoreARGS     ...       // ${datastoreHostsPerSite} * ${wanSites} times
// accessorARGS      ...       // ${accessorHostsPerSite}  * ${wanSites} times
//
hydra.VmPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorvm\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"datastorevm\", ${wanSites}, ${datastoreHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"accessorvm\", ${wanSites}, ${accessorHostsPerSite}, false)"
  ncf;
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"datastorehost\", ${wanSites}, ${datastoreHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"accessorhost\", ${wanSites}, ${accessorHostsPerSite}, false)"
  ncf;
//hydra.VmPrms-extraVMArgs   =
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"locatorARGS\", ${locatorHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//      (\"datastoreARGS\", ${datastoreHostsPerSite} * ${wanSites}, true)"
//  ncf
//  ,
//  fcn "hydra.TestConfigFcns.duplicate
//      (\"accessorARGS\",   ${accessorHostsPerSite} * ${wanSites}, true)"
//  ncf;

//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup per wan site
// and maps each one to the appropriate VM description
// and scales the number of VMs and threads per VM for each.
//
// locator_1_1     locator_1_2    ... locator_1_${locatorHostsPerSite}
// locator_2_1     locator_2_2    ... locator_2_${locatorHostsPerSite}
// ...             ...         locator_${wanSites}_${locatorHostsPerSite}
//
// datastore_1_1   datastore_1_2  ... datastore_1_${datastoreHostsPerSite}
// datastore_2_1   datastore_2_2  ... datastore_2_${datastoreHostsPerSite}
// ...             ...         datastore_${wanSites}_${datastoreHostsPerSite}
//
// accessor_1_1    accessor_1_2   ... accessor_1_${accessorHostsPerSite}
// accessor_2_1    accessor_2_2   ... accessor_2_${accessorHostsPerSite}
// ...             ...         accessor_${wanSites}_${accessorHostsPerSite}
//
// locatorvm_1_1   locatorvm_1_2  ... locatorvm_1_${locatorHostsPerSite}
// locatorvm_2_1   locatorvm_2_2  ... locatorvm_2_${locatorHostsPerSite}
// ...             ...         locatorvm_${wanSites}_${locatorHostsPerSite}
//
// datastorevm_1_1 datastorevm_1_2 ... datastorevm_1_${datastoreHostsPerSite}
// datastorevm_2_1 datastorevm_2_2 ... datastorevm_2_${datastoreHostsPerSite}
// ...             ...         datastorevm_${wanSites}_${datastoreHostsPerSite}
//
// accessorvm_1_1  accessorvm_1_2 ... accessorvm_1_${accessorHostsPerSite}
// accessorvm_2_1  accessorvm_2_2 ... accessorvm_2_${accessorHostsPerSite}
// ...             ...         accessorvm_${wanSites}_${accessorHostsPerSite}
//
// ${locatorThreadsPerVM} ...  // ${wanSites} * ${locatorHostsPerSite} times
// ${datastoreVMsPerHost} ...  // ${datastoreHostsPerSite} * ${wanSites} times
// ${accessorVMsPerHost}  ...  // ${accessorHostsPerSite} * ${wanSites} times
//
hydra.ClientPrms-names =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"datastore\", ${wanSites}, ${datastoreHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"accessor\", ${wanSites}, ${accessorHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorvm\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"datastorevm\", ${wanSites}, ${datastoreHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"accessorvm\", ${wanSites}, ${accessorHostsPerSite}, false)"
  ncf;
hydra.ClientPrms-vmQuantities   =
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorVMsPerHost}\", ${wanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${datastoreVMsPerHost}\", ${datastoreHostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${accessorVMsPerHost}\",   ${accessorHostsPerSite} * ${wanSites})"
  ncf;
hydra.ClientPrms-vmThreads   =
  fcn "hydra.TestConfigFcns.duplicate
       (\"${locatorThreadsPerVM}\", ${wanSites} * ${locatorHostsPerSite})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${datastoreThreadsPerVM}\", ${datastoreHostsPerSite} * ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.duplicate
      (\"${accessorThreadsPerVM}\",   ${accessorHostsPerSite} * ${wanSites})"
  ncf;

//------------------------------------------------------------------------------
// Configures a fabric server description per logical subgroup per wan site
// and maps each one to the appropriate client descriptions
// and puts each WAN site in a different distributed system
// and configures the datastore subgroup to host data.
//
// locator_1     locator_2     ...  locator_${wanSites}
// datastore_1   datastore_2   ...  datastore_${wanSites}
// accessor_1    accessor_2    ...  accessor_${wanSites}
//
// locator_1_1   locator_1_2   ...  locator_1_${locatorHostsPerSite},
// locator_2_1   locator 2_2   ...  locator_2_${locatorHostsPerSite},
// ...           ...         locator_${wanSites}_${locatorHostsPerSite},
//
// datastore_1_1 datastore_1_2 ...  datastore_1_${datastoreHostsPerSite},
// datastore_2_1 datastore_2_2 ...  datastore_2_${datastoreHostsPerSite},
// ...           ...         datastore_${wanSites}_${datastoreHostsPerSite},
//
// accessor_1_1  accessor_1_2  ...  accessor_1_${accessorHostsPerSite},
// accessor_2_1  accessor_2_2  ...  accessor_2_${accessorHostsPerSite},
// ...           ...         accessor_${wanSites}_${accessorHostsPerSite},
//
// ds_1          ds_2          ...  ds_${wanSites}
// ds_1          ds_2          ...  ds_${wanSites}
// ds_1          ds_2          ...  ds_${wanSites}

hydra.gemfirexd.FabricServerPrms-names =
      fcn "hydra.TestConfigFcns.generateNames
           (\"locator_\", ${wanSites})" ncf
      fcn "hydra.TestConfigFcns.generateNames
           (\"datastore_\", ${wanSites})" ncf
      fcn "hydra.TestConfigFcns.generateNames
           (\"accessor_\", ${wanSites})" ncf
      ;
hydra.gemfirexd.FabricServerPrms-clientNames =
      fcn "hydra.TestConfigFcns.generateDoubleSuffixedRunsOfNames
          (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false)"
      ncf
      ,
      fcn "hydra.TestConfigFcns.generateDoubleSuffixedRunsOfNames
          (\"datastore\", ${wanSites}, ${datastoreHostsPerSite}, false)"
      ncf
      ,
      fcn "hydra.TestConfigFcns.generateDoubleSuffixedRunsOfNames
          (\"accessor\", ${wanSites}, ${accessorHostsPerSite}, false)"
      ncf
      ;
hydra.gemfirexd.FabricServerPrms-distributedSystem  =
      fcn "hydra.TestConfigFcns.generateNames
          (\"ds_\", ${wanSites})"
      ncf
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
      fcn "hydra.TestConfigFcns.duplicate
          (\"false\", ${wanSites})"
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
//THREADGROUP datastore
//  totalThreads = fcn ${wanSites} * ${datastoreHostsPerSite}
//                   * ${datastoreVMsPerHost} * ${datastoreThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"datastore\", ${wanSites}, ${datastoreHostsPerSite}, false, true)"
//                 ncf;
//THREADGROUP accessor
//  totalThreads = fcn ${wanSites} * ${accessorHostsPerSite}
//                   * ${accessorVMsPerHost} * ${accessorThreadsPerVM} ncf
//  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
//              (\"accessor\", ${wanSites}, ${accessorHostsPerSite}, false, true)"
//                 ncf;
//------------------------------------------------------------------------------
