hydra.Prms-testDescription += "Objects are DataSerializable DeltaFastAssetAccounts with Instantiator containing ${objectSize} FastAssets each.";

cacheperf.CachePerfPrms-objectType = objects.DeltaFastAssetAccount;
objects.FastAssetAccountPrms-encodeTimestamp = true;
objects.FastAssetAccountPrms-size = ${objectSize};
objects.FastAssetPrms-maxValue = ${maxKeys};
