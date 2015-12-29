hydra.gemfirexd.GfxdConfigPrms-diskStoreConfig = tpch;
hydra.gemfirexd.DiskStorePrms-names = tpch;
hydra.gemfirexd.DiskStorePrms-autoCompact = false;
hydra.gemfirexd.DiskStorePrms-dirNames = tpch;

hydra.VmPrms-extraVMArgs += "-Dgemfire.disk.recoverValuesSync=true";
