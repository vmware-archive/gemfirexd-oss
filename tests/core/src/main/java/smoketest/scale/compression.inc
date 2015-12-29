hydra.Prms-testDescription += "Data is compressed.";

// Members will need the snappy jar
hydra.VmPrms-extraClassPaths += "$GEMFIRE/lib/snappy-java-1.0.4.1.jar";

// Set compression on the test region
hydra.RegionPrms-compressor = com.gemstone.gemfire.compression.SnappyCompressor;
