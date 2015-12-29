//------------------------------------------------------------------------------
// GENERIC TOPOLOGY WITH A LOCATOR SUBGROUP AND TWO NAMED SUBGROUPS
//
// It is up to users of this file to map the groups to fabric server
// distributed systems.
//
// Note that it is also up to the locator VMs to become gemfirexd locators
// through hydra task(s) that invoke:
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
//      A                    name of the first subgroup
//      ${A}Hosts            number of logical hosts in group A
//      ${A}VMsPerHost       number of VMs per host in group A
//      ${A}ThreadsPerVM     number of threads per VM in group A
//      B                    name of the second subgroup
//      ${B}Hosts            number of logical hosts in group B
//      ${B}VMsPerHost       number of VMs per host in group B
//      ${B}ThreadsPerVM     number of threads per VM in group B
//------------------------------------------------------------------------------
hydra.Prms-testTopology = "Generic topology logically divided into a locator subgroup and two named subgroups.  Subgroup locator has ${locatorHosts} hosts, ${locatorVMsPerHost} VMs per host, and ${locatorThreadsPerVM} threads per VM.  Subgroup ${A} has ${${A}Hosts} hosts, ${${A}VMsPerHost} VMs per host, and ${${A}ThreadsPerVM} threads per VM.  Subgroup ${B} has ${${B}Hosts} hosts, ${${B}VMsPerHost} VMs per host, and ${${B}ThreadsPerVM} threads per VM.";

hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

//------------------------------------------------------------------------------
// Configures a host description per logical host in each subgroup.
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     Ahost1        Ahost2        ...  Ahost${${A}Hosts}
//     Bhost1        Bhost2        ...  Bhost${${B}Hosts}
//
hydra.HostPrms-names = fcn "hydra.TestConfigFcns.generateNames
                            (\"locatorhost\", ${locatorHosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${A}host\", ${${A}Hosts})"
                       ncf
                       fcn "hydra.TestConfigFcns.generateNames
                            (\"${B}host\", ${${B}Hosts})"
                       ncf;
//------------------------------------------------------------------------------
// Configures a VM description per logical host per subgroup
// and maps each one to the appropriate host description
// and shows how to give different VM arguments to each subgroup.
//     locatorvm1    locatorvm2    ...  locatorvm${locatorHosts}
//     Avm1          Avm2          ...  Avm${${A}Hosts}
//     Bvm1          Bvm2          ...  Bvm${${B}Hosts}
//     locatorhost1  locatorhost2  ...  locatorhost${locatorHosts}
//     Ahost1        Ahost2        ...  Ahost${${A}Hosts}
//     Bhost1        Bhost2        ...  Bhost${${B}Hosts}
//     LOCATORARGS   LOCATORARGS   ...  LOCATORARGS // ${locatorHosts} times
//     AARGS         AARGS         ...  AARGS // ${${A}Hosts} times
//     BARGS         BARGS         ...  BARGS // ${${B}Hosts} times
//
hydra.VmPrms-names         = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorvm\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}vm\", ${${A}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}vm\", ${${B}Hosts})"
                             ncf;
hydra.VmPrms-hostNames     = fcn "hydra.TestConfigFcns.generateNames
                                  (\"locatorhost\", ${locatorHosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${A}host\", ${${A}Hosts})"
                             ncf
                             fcn "hydra.TestConfigFcns.generateNames
                                  (\"${B}host\", ${${B}Hosts})"
                             ncf;
//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//                                  (\"LOCATORARGS\", ${locatorHosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"AARGS\", ${${A}Hosts}, true)"
//                             ncf
//                             ,
//                             fcn "hydra.TestConfigFcns.duplicate
//                                  (\"BARGS\", ${${B}Hosts}, true)"
//                             ncf;
//------------------------------------------------------------------------------
// Configures a client description per logical host per subgroup
// and maps each one to the appropriate VM description
// and scales the number of VMs and threads per VM for each.
//   locator1        locator2        ...  locator${locatorHosts}
//   A1              A2              ...  A${${A}Hosts}
//   B1              B2              ...  B${${B}Hosts}
//   locatorvm1      locatorvm2      ...  locatorvm${locatorHosts}
//   Avm1            Avm2            ...  Avm${${A}Hosts}
//   Bvm1            Bvm2            ...  Bvm${${B}Hosts}
//   ${locatorVMsPerHost}   ... ${locatorVMsPerHost} // ${locatorHosts} times
//   ${${A}VMsPerHost}      ... ${${A}VMsPerHost} // ${${A}Hosts} times
//   ${${B}VMsPerHost}      ... ${${B}VMsPerHost} // ${${B}Hosts} times
//   ${locatorThreadsPerVM} ... ${locatorThreadsPerVM} // ${locatorHosts} times
//   ${${A}ThreadsPerVM}    ... ${${A}ThreadsPerVM} // ${${A}Hosts} times
//   ${${B}ThreadsPerVM}    ... ${${B}ThreadsPerVM} // ${${B}Hosts} times
//
hydra.ClientPrms-names        = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locator\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}\", ${${B}Hosts})"
                                ncf;
hydra.ClientPrms-vmNames      = fcn "hydra.TestConfigFcns.generateNames
                                     (\"locatorvm\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${A}vm\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.generateNames
                                     (\"${B}vm\", ${${B}Hosts})"
                                ncf;
hydra.ClientPrms-vmQuantities = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorVMsPerHost}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${A}VMsPerHost}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${B}VMsPerHost}\", ${${B}Hosts})"
                                ncf;
hydra.ClientPrms-vmThreads    = fcn "hydra.TestConfigFcns.duplicate
                                  (\"${locatorThreadsPerVM}\", ${locatorHosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${A}ThreadsPerVM}\", ${${A}Hosts})"
                                ncf
                                fcn "hydra.TestConfigFcns.duplicate
                                  (\"${${B}ThreadsPerVM}\", ${${B}Hosts})"
                                ncf;
//------------------------------------------------------------------------------
// Configures a fabric server description per subgroup
// and maps each one to the appropriate client descriptions.
// No distributed system is given.
//
hydra.gemfirexd.FabricServerPrms-names = locator ${A} ${B};
hydra.gemfirexd.FabricServerPrms-clientNames =
      fcn "hydra.TestConfigFcns.generateNames
           (\"locator\", ${locatorHosts})" ncf
      ,
      fcn "hydra.TestConfigFcns.generateNames
           (\"${A}\", ${${A}Hosts})" ncf
      ,
      fcn "hydra.TestConfigFcns.generateNames
           (\"${B}\", ${${B}Hosts})" ncf
      ;
//------------------------------------------------------------------------------
// Example where all subgroups are peers.
//
//hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
//
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
//THREADGROUP ${A}
//    totalThreads = fcn ${${A}Hosts} * ${${A}VMsPerHost}
//                                    * ${${A}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${A}\", ${${A}Hosts}, true)"
//                   ncf;
//THREADGROUP ${B}
//    totalThreads = fcn ${${B}Hosts} * ${${B}VMsPerHost}
//                                    * ${${B}ThreadsPerVM}
//                   ncf
//    clientNames  = fcn "hydra.TestConfigFcns.generateNames
//                        (\"${B}\", ${${B}Hosts}, true)"
//                   ncf;
//------------------------------------------------------------------------------
