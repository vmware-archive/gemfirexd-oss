hydra.Prms-testDescription += "Objects are byte arrays of size ${objectSize}.";

cacheperf.CachePerfPrms-objectType = objects.ArrayOfByte;
objects.ArrayOfBytePrms-encodeKey = true;
objects.ArrayOfBytePrms-encodeTimestamp = true;
objects.ArrayOfBytePrms-size = ${objectSize};
