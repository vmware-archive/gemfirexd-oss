util.RandomValuesPrms-objectType = 
          java.lang.Boolean          java.lang.Byte           java.lang.Character 
          java.lang.Double           java.lang.Float          java.lang.Integer 
          java.lang.Long             java.lang.Short          java.lang.String
          java.math.BigDecimal       java.math.BigInteger     
          java.util.Hashtable        java.util.HashMap       java.util.Vector
          java.util.ArrayList        java.util.BitSet        java.util.HashSet
          java.util.LinkedList       java.util.Properties    java.util.Stack
          java.util.TreeSet          java.util.TreeMap
          java.lang.StringBuffer

          long[] double[] boolean[] byte[] char[] float[] short[]
          ;

util.RandomValuesPrms-keyType = 
          // immutable only
          java.lang.Boolean          java.lang.Byte           java.lang.Character 
          java.lang.Double           java.lang.Float          java.lang.Integer 
          java.lang.Long             java.lang.Short          java.lang.String
          java.math.BigDecimal       java.math.BigInteger     
          ;

util.RandomValuesPrms-valueType = 
          java.lang.Boolean          java.lang.Byte           java.lang.Character 
          java.lang.Double           java.lang.Float          java.lang.Integer 
          java.lang.Long             java.lang.Short          java.lang.String
          java.math.BigDecimal       java.math.BigInteger     
          java.util.Hashtable        java.util.HashMap       java.util.Vector
          java.util.ArrayList        java.util.BitSet        java.util.HashSet
          java.util.LinkedList       java.util.Properties    java.util.Stack
          java.util.TreeSet          java.util.TreeMap
          java.lang.StringBuffer
          long[] double[] boolean[] byte[] char[] float[] short[]
          ;

util.RandomValuesPrms-setElementType =  
     // need to use only classes which do not rely on the elements of the collection to
     // get a hashCode, otherwise collection elements in a set cannot be looked up later
     // if the collection element changes its elements (which changes its hashcode)
          java.lang.Boolean          java.lang.Byte           java.lang.Character 
          java.lang.Double           java.lang.Float          java.lang.Integer 
          java.lang.Long             java.lang.Short          java.lang.String
          java.math.BigDecimal       java.math.BigInteger     
          ;

util.RandomValuesPrms-stringSize = RANGE 1 50 EGNAR;
util.RandomValuesPrms-objectDepth = 5;
util.RandomValuesPrms-elementSize = RANGE 0 5 EGNAR;
util.RandomValuesPrms-borderCasePercentage = 10;
util.RandomValuesPrms-ArrayList_constructor = ONEOF default initialCapacity FOENO;
util.RandomValuesPrms-HashMap_constructor = ONEOF default initialCapacity initialCapacity_loadFactor FOENO;
util.RandomValuesPrms-Hashtable_constructor = ONEOF default initialCapacity initialCapacity_loadFactor FOENO;
util.RandomValuesPrms-LinkedList_constructor = default;
util.RandomValuesPrms-Vector_constructor = ONEOF default initialCapacity 
                                                         initialCapacity_capacityIncrement FOENO;
util.RandomValuesPrms-ArrayList_initialCapacity = RANGE 0 100 EGNAR;
util.RandomValuesPrms-HashMap_initialCapacity = RANGE 0 100 EGNAR;
util.RandomValuesPrms-Hashtable_initialCapacity = RANGE 0 100 EGNAR;
util.RandomValuesPrms-Vector_initialCapacity = RANGE 0 100 EGNAR;
util.RandomValuesPrms-HashMap_loadFactor = 0.5;
util.RandomValuesPrms-Hashtable_loadFactor = 0.5;
util.RandomValuesPrms-Vector_capacityIncrement = RANGE 0 100 EGNAR;
util.RandomValuesPrms-ArrayList_syncWrapper = ONEOF true false FOENO;
util.RandomValuesPrms-HashMap_syncWrapper = ONEOF true false FOENO;
util.RandomValuesPrms-Hashtable_syncWrapper = ONEOF true false FOENO;
util.RandomValuesPrms-LinkedList_syncWrapper = ONEOF true false FOENO;
util.RandomValuesPrms-Vector_syncWrapper = ONEOF true false FOENO;
