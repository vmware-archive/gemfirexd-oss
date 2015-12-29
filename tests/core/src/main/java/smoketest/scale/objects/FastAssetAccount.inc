hydra.Prms-testDescription += "Objects are DataSerializable FastAssetAccounts with Instantiator containing ${objectSize} FastAssets each.";

cacheperf.CachePerfPrms-objectType = objects.FastAssetAccount;
objects.FastAssetAccountPrms-encodeTimestamp = true;
objects.FastAssetAccountPrms-size = ${objectSize};
objects.FastAssetPrms-maxValue = ${maxKeys};
