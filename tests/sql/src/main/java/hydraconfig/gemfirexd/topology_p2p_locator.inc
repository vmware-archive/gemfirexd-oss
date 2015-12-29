//------------------------------------------------------------------------------
// GENERIC TOPOLOGY WITH A LOCATOR SUBGROUP AND ONE SUBGROUP NAMED PEER
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
//------------------------------------------------------------------------------
// PROPERTIES:
//      locatorHosts         number of logical locator hosts
//      locatorVMsPerHost    number of locator VMs per logical locator host
//      locatorThreadsPerVM  number of locator threads per locator VM
//      peerHosts            number of logical peer hosts
//      peerVMsPerHost       number of peer VMs per host
//      peerThreadsPerVM     number of peer threads per VM
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Generic topology logically divided into a locator subgroup and two named subgroups.  Subgroup locator has ${locatorHosts} hosts, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup peer has ${peerHosts} hosts, ${peerVMsPerHost} VMs per host, and ${peerThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     peerhost1     peerhost2     ...  peerhost${peerHosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"locatorhost\", ${locatorHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"peerhost\", ${peerHosts})"
                       ncf;
//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     locatorvm1    locatorvm2    ...  locatorvm${locatorHosts}
//     peervm1       peervm2       ...  peervm${peerHosts}
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     peerhost1     peerhost2     ...  peerhost${peerHosts}
//     LOCATORARGS   LOCATORARGS   ...  LOCATORARGS // ${locatorHosts} times
//     peerARGS      peerARGS      ...  peerARGS // ${peerHosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorvm\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"peervm\", ${peerHosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorhost\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"peerhost\", ${peerHosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"LOCATORARGS\", ${locatorHosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"peerARGS\", ${peerHosts}, true)"
//                             ncf;
//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup
// and maps each one to the appropriate VM description
// and scales the number of VMs and threads per VM for each.
//   locator1   locator2    ...  locator${locatorHosts}
//   peer1      peer2       ...  peer${peerHosts}
//   locatorvm1 locatorvm2  ...  locatorvm${locatorHosts}
//   peervm1    peervm2     ...  peervm${peerHosts}
//   ${locatorVMsPerHost}   ...  ${locatorVMsPerHost} // ${locatorHosts} times
//   ${peerVMsPerHost}      ...  ${peerVMsPerHost} // ${peerHosts} times
//   ${locatorThreadsPerVM} ...  ${locatorThreadsPerVM} // ${locatorHosts} times
//   ${peerThreadsPerVM}    ...  ${peerThreadsPerVM} // ${peerHosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locator\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"peer\", ${peerHosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorvm\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"peervm\", ${peerHosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorVMsPerHost}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${peerVMsPerHost}\", ${peerHosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorThreadsPerVM}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${peerThreadsPerVM}\", ${peerHosts})"
                                ncf;
//------------------------------------------------------------------------------
// Configures a fabric server description per subgroup
// and maps each one to the appropriate client descriptions
// and puts each subgroup in the distributed system "ds"
//
hydra.gemfirexd.FabricServerPrms-names = locator peer;
hydra.gemfirexd.FabricServerPrms-clientNames =
      fcn "hydra.TestConfigFcns.generateNames
           (\"locator\", ${locatorHosts})" ncf
      ,
      fcn "hydra.TestConfigFcns.generateNames
           (\"peer\", ${peerHosts})" ncf
      ;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;

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
//THREADGROUP peer
//    totalThreads = fcn ${peerHosts} * ${peerVMsPerHost}
//                                    * ${peerThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"peer\", ${peerHosts}, true)"
//                   ncf;
//------------------------------------------------------------------------------
