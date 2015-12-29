// randomize the use of compression
hydra.VmPrms-extraClassPaths += "$GEMFIRE/lib/snappy-java-1.0.4.1.jar";
hydra.GemFirePrms-extraLocatorClassPath += "$GEMFIRE/lib/snappy-java-1.0.4.1.jar";
hydra.RegionPrms-compressionCodec = fcn util.CompressionHydraFcn.randomCompressionCodec("SNAPPY1_0_4") ncf;

