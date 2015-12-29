// include file for off heap memory
//    - topology group "A" does not have off-heap configured, topology group "B" does have off-heap configured
//    - first "column" for hydra.RegionPrms configuration has no off-heap, all other columns do have off-heap
//    - validation in close tasks use default thread group
hydra.GemFirePrms-offHeapMemorySize= fcn "hydra.TestConfigFcns.duplicate(\"0m\", ${${A}Hosts} * ${${A}VMsPerHost})" ncf
                                     fcn "hydra.TestConfigFcns.duplicate(\"350m\", ${${B}Hosts} * ${${B}VMsPerHost})" ncf;
hydra.RegionPrms-enableOffHeapMemory= false true;

// off heap validation needs synchronous disk recovery to prevent memory rattling during validation
hydra.VmPrms-extraVMArgs += "-Dgemfire.disk.recoverValuesSync=true";

// one final check for off-heap memory, then close the cache and check again to verify
// that there are no orphaned values
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = closeAllRegions;
CLOSETASK taskClass = memscale.OffHeapHelper taskMethod = verifyOffHeapMemoryConsistencyOnce;

