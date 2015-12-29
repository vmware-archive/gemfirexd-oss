hydra.HostPrms-names = host1 host2;
  
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.poolList
      (\"host1, host2\", ${locatorHostsPerSite}, ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.poolList
      (\"host1, host2\", ${peerHostsPerSite}, ${wanSites})"
  ncf;
  
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.poolList
      (\"host1, host2\", ${locatorHostsPerSite}, ${wanSites})"
  ncf
  fcn "hydra.TestConfigFcns.poolList
      (\"host1, host2\", ${peerHostsPerSite}, ${wanSites})"
  ncf;
  