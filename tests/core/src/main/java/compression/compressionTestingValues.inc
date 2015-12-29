util.TestHelperPrms-minTaskGranularitySec = 5; // length of ops per region

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType  = java.lang.Boolean   boolean boolean[]
                                    java.lang.Byte      byte    byte[]
                                    java.lang.Character char    char[]
                                    java.lang.Double    double  double[]
                                    java.lang.Float     float   float[]
                                    java.lang.Integer   int     int[]
                                    java.lang.Long      long    long[]
                                    java.lang.Short     short   short[]
                                    java.lang.String
                                    java.lang.StringBuffer
                                    java.math.BigDecimal
                                    java.math.BigInteger
                                    java.util.TreeMap
                                    java.util.ArrayList
                                    java.util.TreeSet
                                    ;
util.RandomValuesPrms-stringSize  = RANGE 100 500 EGNAR;
util.RandomValuesPrms-elementSize = 5;
util.RandomValuesPrms-objectDepth = ONEOF 0 1 FOENO;

util.ValueHolderPrms-useExtraObject = false;

// for compression
hydra.GemFirePrms-extraLocatorClassPath += $GEMFIRE/lib/snappy-java-1.0.4.1.jar;
hydra.VmPrms-extraClassPaths            += $GEMFIRE/lib/snappy-java-1.0.4.1.jar;
