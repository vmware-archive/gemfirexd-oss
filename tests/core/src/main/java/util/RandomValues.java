/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
   
package util;

import hydra.Log;
import hydra.TestConfig;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.lang.reflect.*;
import java.util.*;

/**
 * A class for generating random data used in testing.
 *
 * @see RandomValuesPrms
 *
 * @author Lynn Gallinat
 * @since 1.0
 */
public class RandomValues implements java.io.Serializable {

// static vars
private static char[] printableChars;
private static Class[] factoryClasses;

public boolean createDebug = false;
public boolean modifyDebug = true;
public boolean logProgress = false;

// used to generate Strings
final int NORMAL_CASE = 0;
final int BORDER_CASE_MIN_VALUE = 1;
final int BORDER_CASE_MAX_VALUE = 2;
final int BORDER_CASE_ALT_VALUE = 3;

// Cache some of the hydra values
Vector objectTypeVec;                    // list of classes to choose a random object from;
Vector keyTypeVec;                       // list of classes to choose a random dict key from;
Vector valueTypeVec;                     // list of classes to choose a random dict value from;
Vector setElementVec;                    // list of classes to choose a random set value from;

/** Create randomValues instance using the hydra params in RandomValuesPrms.
 */
public RandomValues() {
   initialize();
}

/** Create randomValues using the specified hydra config variable for
 *  the RandomValuesPrms.objectType parameter. All other hydra paramaters
 *  are as specified in RandomValuesPrms.
 */
public RandomValues(Long objectType_paramKey) {
   initialize();
   objectTypeVec = TestConfig.tab().vecAt(objectType_paramKey);
}

static {
  setPrintableChars(false);
}

public static void setPrintableChars(boolean lettersAndDigitsOnly) {
  char[] otherchars = null;
  if (lettersAndDigitsOnly) {
    otherchars = new char[0];
  } else {
    otherchars = new char[] {'!', '@', '#', '$', '%', '^', '&', '*', '(', ')', '-', '_', 
      '=', '+', '\\', '|', '`', '~', '{', '[', ']', ';', ':', '\'', '"', '<', ',', 
      '>', '.', '?', '/'};
  }
  int printableCharsLength = ('9' - '0' + 1) +
      ('Z' - 'A' + 1) +
      ('z' - 'a' + 1) +
      otherchars.length;          
  printableChars = new char[printableCharsLength];
  int index = 0;
  for (int i = '0'; i <= '9'; i++)
    printableChars[index++] = (char)i;
  for (int i = 'A'; i <= 'Z'; i++)
    printableChars[index++] = (char)i;
  for (int i = 'a'; i <= 'z'; i++)
    printableChars[index++] = (char)i;
  for (int i = 0; i < otherchars.length; i++)
    printableChars[index++] = otherchars[i]; 
  if (index != printableCharsLength)
    throw new TestException("expected index " + index + " to be " + printableCharsLength);
}

void initialize() {
   objectTypeVec = TestConfig.tab().vecAt(RandomValuesPrms.objectType, null);
   keyTypeVec = TestConfig.tab().vecAt(RandomValuesPrms.keyType, null);
   valueTypeVec = TestConfig.tab().vecAt(RandomValuesPrms.valueType, null);
   setElementVec = TestConfig.tab().vecAt(RandomValuesPrms.setElementType, null);
}
       
//****************************************************************************
// public methods to create random values/objects

/** Return a random object, using the Hydra param keys specified in this
 *  instance of RandomValues. The returned object contains no elements.
 *
 *  @return Return a random object.
 */
public Object getRandomObject() {
   String objectType = getRandomStringFromVec(getObjectTypeVec());
   Object anObj = getRandomObjectForType(objectType);
   return anObj;
}

/** Return a random object, using the Hydra param keys specified in this
 *  instance of RandomValues. The returned object contains no elements.
 *
 *  @param extraObj The special object used if the objectType chooses 
 *         the special String "this", typically used to create circular 
 *         references in objects (extraObj would be the collection that the
 *         random object will be added to)
 * 
 *  @return Return a random object of the specified type.
 */
public Object getRandomObject(Object extraObj) {
   String objectType = getRandomStringFromVec(getObjectTypeVec());
   Object anObj = getRandomObjectForType(objectType, extraObj);
   return anObj;
}

/** Return a random object graph, using the Hydra param keys specified in this
 *  instance of RandomValues.
 *
 *  @return Return a random object of the specified type.
 */
public Object getRandomObjectGraph() {
   String objectType = getRandomStringFromVec(getObjectTypeVec());
   Object anObj = getRandomObjectForType(objectType);
   createDepth(anObj);
   return anObj;
}

/** Return a random object graph, using the Hydra param keys specified in this
 *  instance of RandomValues.
 *
 *  @param extraObj The special object used if the objectType chooses 
 *         the special String "this", typically used to create circular 
 *         references in objects (extraObj would be the collection that the
 *         random object will be added to)
 * 
 *  @return Return a random object of the specified type.
 */
public Object getRandomObjectGraph(Object extraObj) {
   String objectType = getRandomStringFromVec(getObjectTypeVec());
   Object anObj = getRandomObjectForType(objectType, extraObj);
   createDepth(anObj);
   return anObj;
}

/** Return a random object with the given className, using the Hydra param keys 
 *  specified in this instance of RandomValues.
 *
 *  @param className The class of the object to create.
 * 
 *  @return Return a random object of the specified type.
 */
public Object getRandomObject(String className) {
   return getRandomObjectForType(className);
}

/** Return a random object graph with the given className, using the Hydra param keys 
 *  specified in this instance of RandomValues.
 *
 *  @param className The class of the object to create.
 * 
 *  @return Return a random object of the specified type.
 */
public Object getRandomObjectGraph(String className) {
   Object anObj = getRandomObjectForType(className);
   createDepth(anObj);
   return anObj;
}

// ================================================================================

/** Return a random object of the type specified with the given class name.
 *
 *  @param dataTypeName A fully qualified class name.
 *  @param extraObj The special object used if the dateTypeName is the special 
 *         String "this".
 *
 *  @return Return a random object of the specified type.
 */
Object getRandomObjectForType(String dataTypeName, Object extraObj) {
   Object returnObject = null;
   if (dataTypeName.equals("this"))
      returnObject = extraObj;
   else 
      returnObject = getRandomObjectForType(dataTypeName);
   return returnObject;
}

/** Return a random object of the type specified with the given class name.
 *
 *  @param dataTypeName A fully qualified class name.
 *
 *  @return Return a random object of the specified type.
 */
Object getRandomObjectForType(String dataTypeName) {
   Object returnObject = null;
   if (dataTypeName.equals(Integer.class.getName()) || dataTypeName.equals("int")) {
      returnObject = new Integer(getRandom_int());
   } else if (dataTypeName.equals(String.class.getName())) {
      returnObject = getRandom_String();
   } else if (dataTypeName.equals(Long.class.getName()) || dataTypeName.equals("long")) {
      returnObject = new Long(getRandom_long());
   } else if (dataTypeName.equals(Double.class.getName()) || dataTypeName.equals("double")) {
      returnObject = new Double(getRandom_double());
   } else if (dataTypeName.equals(Boolean.class.getName()) || dataTypeName.equals("boolean")) {
      returnObject = new Boolean(getRandom_boolean());
   } else if (dataTypeName.equals(Byte.class.getName()) || dataTypeName.equals("byte")) {
      returnObject = new Byte(getRandom_byte());
   } else if (dataTypeName.equals(Character.class.getName()) || dataTypeName.equals("char")) {
      returnObject = new Character(getRandom_char());
   } else if (dataTypeName.equals(Float.class.getName()) || dataTypeName.equals("float")) {
      returnObject = new Float(getRandom_float());
   } else if (dataTypeName.equals(Short.class.getName()) || dataTypeName.equals("short")) {
      returnObject = new Short(getRandom_short());
   } else if (dataTypeName.equals(BigInteger.class.getName())) {
      returnObject = getRandom_BigInteger();
   } else if (dataTypeName.equals(BigDecimal.class.getName())) {
      returnObject = getRandom_BigDecimal();
   } else if (dataTypeName.equals(StringBuffer.class.getName())) {
      returnObject = getRandom_StringBuffer();
   } else if (dataTypeName.equals("null")) {
      returnObject = null;
   } else if (dataTypeName.equals("this")) {
      throw new TestException("Problem in test: dataTypeName is this, but no extra object supplied");
   } else if (dataTypeName.endsWith("[]")) {
      returnObject = getRandom_array(dataTypeName);
   } else {
      returnObject = getRandom_Object(dataTypeName);
   }
   return returnObject;
}

/** Return a random int, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random int.
 */
public int getRandom_int() {
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      return getRandom_intBorderCase();
   }
   return TestConfig.tab().getRandGen().nextInt();
}

/** Return a random border case int (ie min/max etc).
 *
 *  @return A random border case int.
 */
public int getRandom_intBorderCase() {
   int[] anArr = new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE, 0};
   return anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
}

/** Return a random float, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random float.
 */
public float getRandom_float() {
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      return getRandom_floatBorderCase();
   }
   return TestConfig.tab().getRandGen().nextFloat();
}

/** Return a random border case float (ie min/max etc).
 *
 *  @return A random border case float.
 */
public float getRandom_floatBorderCase() {
      float[] anArr = new float[] {Float.MIN_VALUE, Float.MAX_VALUE, -Float.MIN_VALUE, 
                      -Float.MAX_VALUE, Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, 
                      (float)0.0};
      return anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
   }

/** Return a random double, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random double.
 */
public double getRandom_double() {
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      return getRandom_doubleBorderCase();
   }
   return TestConfig.tab().getRandGen().nextDouble();
}

/** Return a random border case double (ie min/max etc).
 *
 *  @return A random border case double.
 */
public double getRandom_doubleBorderCase() {
      double[] anArr = new double[] {Double.MIN_VALUE, Double.MAX_VALUE, -Double.MIN_VALUE, 
                       -Double.MAX_VALUE, Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 
                       0.0};
      return anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
   }

/** Return a random boolean.
 *
 *  @return A random boolean.
 */
public boolean getRandom_boolean() {
   return TestConfig.tab().getRandGen().nextBoolean();
}

/** Return a random byte, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random byte.
 */
public byte getRandom_byte() {
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      return getRandom_byteBorderCase();
   }
   byte[] byteArr = new byte[1];
   TestConfig.tab().getRandGen().nextBytes(byteArr);
   return byteArr[0];
}

/** Return a random border case byte (ie min/max etc).
 *
 *  @return A random border case byte.
 */
public byte getRandom_byteBorderCase() {
   byte[] anArr = new byte[] {Byte.MIN_VALUE, Byte.MAX_VALUE, (byte)0};
   return anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
}

/** Return a random array of bytes.  Does not use the hydra parameter
 *  borderCasePercentage to randomly choose border case values
 *  (i.e. min/max etc).
 *
 *  @return A random array of bytes.
 */
public byte[] getRandom_arrayOfBytes() {
   int arraySize = TestConfig.tasktab().intAt(RandomValuesPrms.elementSize,
                   TestConfig.tab().intAt(RandomValuesPrms.elementSize));
   byte[] byteArr = new byte[arraySize];
//   TestConfig.tab().getRandGen().nextBytes(byteArr);
   return byteArr;
}

/** Return a random char, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random char.
 */
public char getRandom_char() {
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      return getRandom_charBorderCase();
   }
   return TestConfig.tab().getRandGen().nextChar();
}

/** Return a random border case char (ie min/max etc).
 *
 *  @return A random border case char.
 */
public char getRandom_charBorderCase() {
   char[] anArr = new char[] {Character.MIN_VALUE, Character.MAX_VALUE};
   return anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
}

/** Return a random long, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random long.
 */
public long getRandom_long() {
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      return getRandom_longBorderCase();
   }
   return TestConfig.tab().getRandGen().nextLong();
}

/** Return a random border case long (ie min/max etc).
 *
 *  @return A random border case long.
 */
public long getRandom_longBorderCase() {
   long[] anArr = new long[] {Long.MIN_VALUE, Long.MAX_VALUE, 0};
   return anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
}

/** Return a random short, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random short.
 */
public short getRandom_short() {
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      return getRandom_shortBorderCase();
   }
   return (short)getRandom_int();
}

/** Return a random border case short (ie min/max etc).
 *
 *  @return A random border case short.
 */
public short getRandom_shortBorderCase() {
      short[] anArr = new short[] {Short.MIN_VALUE, Short.MAX_VALUE, (short)0};
      return anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
   }

/** Return a random String, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc). The string will contain
 *  printable characters (other than the border case).
 *
 *  doubleChar Character to double if used (if the returned string is to be
 *             used within quotes, this will allow a string to contain the
 *             quote character).
 *  @return A random String.
 */
public String getRandom_String(char doubleChar, int size) {
  return getRandom_String(doubleChar, (long)size);
}

/** Return a random String, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc). The string will contain
 *  printable characters (other than the border case).
 *
 *  doubleChar Character to double if used (if the returned string is to be
 *             used within quotes, this will allow a string to contain the
 *             quote character).
 *  @return A random String.
 */
public String getRandom_String(char doubleChar, long size) {
   int whichCase = NORMAL_CASE;
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      int[] anArr = new int[] {BORDER_CASE_MIN_VALUE, BORDER_CASE_MAX_VALUE, 
                   BORDER_CASE_ALT_VALUE};
      whichCase = anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
}
   return getRandomString(whichCase, new Character(doubleChar), size);
}

/** Return a random String, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc). The string will contain
 *  printable characters (other than the border case).
 *
 *  @return A random String.
 */
public String getRandom_String() {
   int whichCase = NORMAL_CASE;
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      int[] anArr = new int[] {BORDER_CASE_MIN_VALUE, BORDER_CASE_MAX_VALUE, 
                   BORDER_CASE_ALT_VALUE};
      whichCase = anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
   }
   return getRandomString(whichCase, null, -1);
}

/** Return a random border case String (ie min/max etc). 
 *
 *  @return A random border case String.
 */
public String getRandom_StringBorderCase() {
   int[] anArr = new int[] {BORDER_CASE_MIN_VALUE, BORDER_CASE_MAX_VALUE, 
                            BORDER_CASE_ALT_VALUE};
   int whichCase = anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
   return getRandomString(whichCase, null, -1);
}

/** Return a random string.
 *  
 *  @param whichCase Can be one of:
 *         NORMAL_CASE
 *         BORDER_CASE_MIN_VALUE 
 *         BORDER_CASE_MAX_VALUE
 *         BORDER_CASE_ALT_VALUE
 *  doubleChar Character to double if used (if the returned string is to be
 *             used within quotes, this will allow a string to contain the
 *             quote character).
 *  size The desired size of the string, or use RandomValuesPrms.stringSize
 *       if this is -1.
 */
protected String getRandomString(int whichCase, Character doubleChar, long size) {
   StringBuffer aStr = new StringBuffer();
   long stringSize = size;
   if (stringSize == -1) {
      stringSize = TestConfig.tab().intAt(RandomValuesPrms.stringSize);
   }
   while (aStr.length() < stringSize) {
      if (whichCase == NORMAL_CASE) {
         char aChar = printableChars[TestConfig.tab().getRandGen().nextInt(printableChars.length - 1)];
         if (doubleChar != null) {
            if (aChar == doubleChar.charValue()) {
               aStr.append(aChar);
            }
         }
         aStr.append(aChar);
      } else if (whichCase == BORDER_CASE_MIN_VALUE) {
         aStr.append(Character.MIN_VALUE);
      } else if (whichCase == BORDER_CASE_MAX_VALUE) {
         aStr.append(Character.MAX_VALUE);
      } else {
         if ((aStr.length() % 2) == 0) {
            aStr.append(Character.MIN_VALUE);
         } else {
            aStr.append(Character.MAX_VALUE);
         }
      }
   }
   return aStr.toString();
}

/** Return a random StringBuffer, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc). The StringBuffer will contain
 *  printable characters (other than the border case).
 *
 *  @return A random StringBuffer.
 */
public StringBuffer getRandom_StringBuffer() {
   return new StringBuffer(getRandom_String());
}

/** Return a random BigInteger, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random BigInteger.
 */
public BigInteger getRandom_BigInteger() {
   // limit num bits on a BigInteger, otherwise BigInteger and toString take a long time
   final int MAX_BITS = 50; 
   BigInteger bigInt;
   if (TestHelper.borderCase(RandomValuesPrms.borderCasePercentage)) {
      BigInteger[] anArr = new BigInteger[] {BigInteger.ZERO, BigInteger.ONE,
                new BigInteger(MAX_BITS, TestConfig.tab().getRandGen())};
      bigInt = anArr[TestConfig.tab().getRandGen().nextInt(anArr.length - 1)];
   } else {
      bigInt = new BigInteger(TestConfig.tab().getRandGen().nextInt(1, MAX_BITS),
                   TestConfig.tab().getRandGen());
   }
   if (TestConfig.tab().getRandGen().nextBoolean()) {
      bigInt = new BigInteger("-" + bigInt);
   }
   return bigInt;
}

/** Return a random BigDecimal, using the hydra parameter borderCasePercentage to
 *  randomly choose border case values (ie min/max etc)
 *
 *  @return A random BigDecimal.
 */
public BigDecimal getRandom_BigDecimal() {
   // limit scal on a BigDecimal, otherwise toString gets exceptions, takes a long time, etc.
   BigInteger bigInt = getRandom_BigInteger(); // this call will consider border cases
   String bigIntStr = bigInt.toString();
   int[] scalArr = new int[] {0, bigIntStr.length(), bigIntStr.length() - 1, bigIntStr.length() + 1,
                   TestConfig.tab().getRandGen().nextInt(1, 2 * bigIntStr.length())};
   int scale = scalArr[TestConfig.tab().getRandGen().nextInt(scalArr.length - 1)];
   BigDecimal bigDec = new BigDecimal(bigInt, scale);
   return bigDec;
}

/** Return a random Object of the given class, using a no arg constructor,
 *  or a constructor with a length (int) arg. The length is determined by
 *  the elementSize hydra config param.
 *
 *  @param dataTypeName The fully qualified name of a class.
 *
 *  @return A new instance of dataTypeName.
 */
public Object getRandom_Object(String dataTypeName) {
   Class aClass = null;
   try {
      aClass = Class.forName(dataTypeName);
   } catch (java.lang.ClassNotFoundException anExcept) {
      throw new TestException("Class name " + dataTypeName + " " + anExcept.toString());
   }
   if (Set.class.isAssignableFrom(aClass)) {
      return getRandom_Set(aClass);
   } else if (Map.class.isAssignableFrom(aClass)) {
      return getRandom_Map(aClass);
   } else if (List.class.isAssignableFrom(aClass)) {
      return getRandom_List(aClass);
   } 
   try {
      Object anObj = aClass.newInstance();
      return anObj;
   } catch (java.lang.InstantiationException anExcept) {
      // try a constructor with length arg
      Constructor constructor = null;
      try {
         constructor = aClass.getDeclaredConstructor(new Class[] {int.class});
      } catch (NoSuchMethodException e) { 
         throw new TestException("Could not find a constructor for " + dataTypeName);
      }
      int length = TestConfig.tab().intAt(RandomValuesPrms.elementSize);
      try {
         Object anObj = constructor.newInstance(new Object[] {new Integer(length)});
         return anObj;
      } catch (java.lang.InstantiationException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (java.lang.IllegalAccessException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } catch (java.lang.reflect.InvocationTargetException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   } catch (java.lang.IllegalAccessException anExcept) {
      throw new TestException(TestHelper.getStackTrace(anExcept));
   } catch (Exception anExcept) {
      throw new TestException(TestHelper.getStackTrace(anExcept));
   }
}

public Set getRandom_Set(Class aClass) {
   Set aSet = null;
   boolean syncWrapper = false;
   if (aClass == java.util.TreeSet.class) {
      aSet = new TreeSet(new GenericComparator()); 
   } else {
      aSet = (Set)createInstance(aClass);
   }
   return aSet;
}

public Map getRandom_Map(Class aClass) {
   Map aMap = null;
   boolean syncWrapper = false;
   if (aClass == java.util.TreeMap.class) {
      aMap = new TreeMap(new GenericComparator()); 
   } else if (aClass == java.util.HashMap.class) {
      String constructor = TestConfig.tab().stringAt(RandomValuesPrms.HashMap_constructor);
      HashMap aHM;
      if (constructor.equalsIgnoreCase("default")) {
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new HashMap()...");
         aHM = new HashMap();
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aHM));
      } else if (constructor.equalsIgnoreCase("initialCapacity")) {
         int initialCapacity = TestConfig.tab().intAt(RandomValuesPrms.HashMap_initialCapacity);
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new HashMap(" +
             initialCapacity + ")...");
         aHM = new HashMap(initialCapacity);
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aHM));
      } else if (constructor.equalsIgnoreCase("initialCapacity_loadFactor")) {
         int initialCapacity = TestConfig.tab().intAt(RandomValuesPrms.HashMap_initialCapacity);
         float loadFactor = (float)TestConfig.tab().doubleAt(RandomValuesPrms.HashMap_loadFactor);
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new HashMap(" +
             initialCapacity + ", " + loadFactor + ")...");
         aHM = new HashMap(initialCapacity, loadFactor);
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aHM));
      } else {
         throw new TestException("Unknown constructor " + constructor);
      }
      syncWrapper = TestConfig.tab().booleanAt(RandomValuesPrms.HashMap_syncWrapper);
      aMap = aHM;
   } else if (aClass == java.util.Hashtable.class) {
      String constructor = TestConfig.tab().stringAt(RandomValuesPrms.Hashtable_constructor);
      Hashtable aHT;
      if (constructor.equalsIgnoreCase("default")) {
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new Hashtable()...");
         aHT = new Hashtable();
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aHT));
      } else if (constructor.equalsIgnoreCase("initialCapacity")) {
         int initialCapacity = TestConfig.tab().intAt(RandomValuesPrms.Hashtable_initialCapacity);
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new Hashtable(" +
             initialCapacity + ")...");
         aHT = new Hashtable(initialCapacity);
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aHT));
      } else if (constructor.equalsIgnoreCase("initialCapacity_loadFactor")) {
         int initialCapacity = TestConfig.tab().intAt(RandomValuesPrms.Hashtable_initialCapacity);
         float loadFactor = (float)TestConfig.tab().doubleAt(RandomValuesPrms.Hashtable_loadFactor);
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new Hashtable(" +
             initialCapacity + ", " + loadFactor + ")...");
         aHT = new Hashtable(initialCapacity, loadFactor);
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aHT));
      } else {
         throw new TestException("Unknown constructor " + constructor);
      }
      syncWrapper = TestConfig.tab().booleanAt(RandomValuesPrms.Hashtable_syncWrapper);
      aMap = aHT;
   } else {
      aMap = (Map)createInstance(aClass);
   }
   return aMap;
}

public List getRandom_List(Class aClass) {
   List aList = null;
   boolean syncWrapper = false;
   if (aClass == java.util.ArrayList.class) {
      String constructor = TestConfig.tab().stringAt(RandomValuesPrms.ArrayList_constructor);
      if (constructor.equalsIgnoreCase("default")) {
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new ArrayList()...");
         aList = new ArrayList();
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aList));
      } else if (constructor.equalsIgnoreCase("initialCapacity")) {
         int initialCapacity = TestConfig.tab().intAt(RandomValuesPrms.ArrayList_initialCapacity);
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new ArrayList(" +
             initialCapacity + ")...");
         aList = new ArrayList(initialCapacity);
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aList));
      } else {
         throw new TestException("Unknown constructor " + constructor);
      }
      syncWrapper = TestConfig.tab().booleanAt(RandomValuesPrms.ArrayList_syncWrapper);
   } else if (aClass == java.util.LinkedList.class) {
      String constructor = TestConfig.tab().stringAt(RandomValuesPrms.LinkedList_constructor);
      if (constructor.equalsIgnoreCase("default")) {
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new LinkedList()...");
         aList = new LinkedList();
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aList));
      } else {
         throw new TestException("Unknown constructor " + constructor);
      }
      syncWrapper = TestConfig.tab().booleanAt(RandomValuesPrms.LinkedList_syncWrapper);
   } else if (aClass == Vector.class) {
      String constructor = TestConfig.tab().stringAt(RandomValuesPrms.Vector_constructor);
      if (constructor.equalsIgnoreCase("default")) {
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new Vector()...");
         aList = new Vector();
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aList));
      } else if (constructor.equalsIgnoreCase("initialCapacity")) {
         int initialCapacity = TestConfig.tab().intAt(RandomValuesPrms.Vector_initialCapacity);
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new Vector(" +
             initialCapacity + ")...");
         aList = new Vector(initialCapacity);
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aList));
      } else if (constructor.equalsIgnoreCase("initialCapacity_capacityIncrement")) {
         int initialCapacity = TestConfig.tab().intAt(RandomValuesPrms.Vector_initialCapacity);
         int capacityIncrement = TestConfig.tab().intAt(RandomValuesPrms.Vector_capacityIncrement);
         if (logProgress) Log.getLogWriter().info("Creating instance with constructor: new Vector(" +
             initialCapacity + ", " + capacityIncrement + ")...");
         aList = new Vector(initialCapacity, capacityIncrement);
         if (logProgress) Log.getLogWriter().info("Finished call to constructor: " + TestHelper.toString(aList));
      } else {
         throw new TestException("Unknown constructor " + constructor);
      }
      syncWrapper = TestConfig.tab().booleanAt(RandomValuesPrms.Vector_syncWrapper);
   } else {
      aList = (List)createInstance(aClass);
   }
   return aList;
}

Object createInstance(Class aClass) {
   try {
      Object anObj = aClass.newInstance();
      return anObj;
   } catch (java.lang.InstantiationException anExcept) {
      throw new TestException(TestHelper.getStackTrace(anExcept));
   } catch (java.lang.IllegalAccessException anExcept) {
      throw new TestException(TestHelper.getStackTrace(anExcept));
   } catch (Exception anExcept) {
      throw new TestException(TestHelper.getStackTrace(anExcept));
   }
}

public Object getRandom_array(String type) {
   if (!type.endsWith("[]"))
      throw new TestException("Unable to create array of type " + type);
   String baseType = type.substring(0, type.indexOf("[]"));
   int arraySize = TestConfig.tasktab().intAt(RandomValuesPrms.elementSize,
                   TestConfig.tab().intAt(RandomValuesPrms.elementSize));
   if (baseType.equals("int"))
      return new int[arraySize];
   if (baseType.equals("long"))
      return new long[arraySize];
   if (baseType.equals("double"))
      return new double[arraySize];
   if (baseType.equals("boolean"))
      return new boolean[arraySize];
   if (baseType.equals("byte"))
      return new byte[arraySize];
   if (baseType.equals("char"))
      return new char[arraySize];
   if (baseType.equals("float")) 
      return new float[arraySize];
   if (baseType.equals("short"))
      return new short[arraySize];
   Class aClass = null;
   try {
      aClass = Class.forName(baseType);
   } catch (java.lang.ClassNotFoundException anExcept) {
      throw new TestException(anExcept.toString());
   }
   Object anArr = Array.newInstance(aClass, arraySize);
   return anArr;
}

//****************************************************************************
// methods to create depth in an object

/** Add elements to anObj, creating an object graph with depth specified by 
 *  using the Hydra param keys specified in this instance of RandomValues. 
 *
 *
 * @see RandomValuesPrms#objectDepth
 */
public void createDepth(Object anObj) {
   if (createDebug) Log.getLogWriter().info("in top createDepth with " + TestHelper.toString(anObj));
   int objectDepth = TestConfig.tab().intAt(RandomValuesPrms.objectDepth);
   createDepth(objectDepth, anObj);
   if (createDebug) Log.getLogWriter().info("leaving top createDepth with " + TestHelper.toString(anObj));
}

private void createDepth(int currentDepth, Object anObj) {
   if (createDebug) {
      if (anObj == null)
         Log.getLogWriter().info("in createDepth to sort classes with currentDepth " + currentDepth + ", null");
      else
         Log.getLogWriter().info("in createDepth to sort classes with currentDepth " + currentDepth + anObj.getClass().getName());
   }
   if (currentDepth <= 0) {
   if (createDebug) Log.getLogWriter().info("in createDepth to sort classes, returning because depth is " + currentDepth);
      return;
   }
   if (anObj instanceof Map) {
      Map aMap = (Map)anObj;
      if (createDebug) Log.getLogWriter().info("About to call createDepthForMap");
      createDepthForMap(currentDepth, aMap);
   } else if (anObj instanceof List) {
      List aList = (List)anObj;
      if (createDebug) Log.getLogWriter().info("About to call createDepthForList");
      createDepthForList(currentDepth, aList);
   } else if (anObj instanceof Set) {
      Set aSet = (Set)anObj;
      if (createDebug) Log.getLogWriter().info("About to call createDepthForSet");
      createDepthForSet(currentDepth, aSet);
   } else if (anObj.getClass().isArray()) {
      if (createDebug) Log.getLogWriter().info("About to call createDepthForArray");
      createDepthForArray(currentDepth, anObj);
   } else if (anObj instanceof BitSet) {
      if (createDebug) Log.getLogWriter().info("Creating depth for BitSet");
      BitSet bSet = (BitSet)anObj;
      for (int i = 0; i < bSet.length(); i++) {
         if (TestConfig.tab().getRandGen().nextBoolean())
            bSet.set(i);
         else
            bSet.clear(i);
      }
   } else if ((anObj instanceof Boolean) || (anObj instanceof Byte) || (anObj instanceof Character) ||
              (anObj instanceof Double) || (anObj instanceof Float) || (anObj instanceof Integer) ||
              (anObj instanceof Long) || (anObj instanceof Short) || (anObj instanceof String) ||
              (anObj instanceof BigDecimal) || (anObj instanceof BigInteger) || (anObj instanceof StringBuffer)) {
     // classes that cannot have depth
     return;
   } else {
      throw new TestException("Don't know how to create depth for " + anObj.getClass().getName());
   }
}

private void createDepthForMap(int currentDepth, Map aMap) {
   if (createDebug) Log.getLogWriter().info("in createDepthForMap currentDepth " + currentDepth + ", " + TestHelper.toString(aMap));
   int elementSize = TestConfig.tab().intAt(RandomValuesPrms.elementSize);
   if (createDebug) Log.getLogWriter().info("elementSize is " + elementSize);
   while (aMap.size() < elementSize) {
      if (createDebug) Log.getLogWriter().info("top of loop");
      Object key = getRandomObject(getKeyTypeVec(), aMap);
      if (createDebug) Log.getLogWriter().info("key is " + key);
      Object value = getRandomObject(getValueTypeVec(), aMap);
      if (createDebug) Log.getLogWriter().info("value is " + value.getClass().getName());
      aMap.put(key, value);
      if (createDebug) Log.getLogWriter().info("in createDepthForMap putting key " + TestHelper.toString(key) + ", value " + TestHelper.toString(value));
      if (createDebug) Log.getLogWriter().info("calling createdepth for key");
      createDepth(currentDepth-1, key);
      if (createDebug) Log.getLogWriter().info("calling createdepth for value");
      createDepth(currentDepth-1, value);
      if (createDebug) Log.getLogWriter().info("at end of loop");
   }
   if (createDebug) Log.getLogWriter().info("outside of loop");
}
  
private void createDepthForList(int currentDepth, List aList) {
   if (createDebug) Log.getLogWriter().info("in createDepthForList currentDepth " + currentDepth + ", " + TestHelper.toString(aList));
   int elementSize = TestConfig.tab().intAt(RandomValuesPrms.elementSize);
   while (aList.size() < elementSize) {
      Object anObj = getRandomObject(getObjectTypeVec());
      aList.add(anObj);
      if (createDebug) Log.getLogWriter().info("in createDepthForList putting ele " + TestHelper.toString(anObj));
      createDepth(currentDepth-1, anObj);
   }
}
  
private void createDepthForSet(int currentDepth, Set aSet) {
   int elementSize = TestConfig.tab().intAt(RandomValuesPrms.elementSize);
   while (aSet.size() < elementSize) {
      Object anObj = getRandomObject(getSetElementTypeVec());
      aSet.add(anObj);
      createDepth(currentDepth-1, anObj);
   }
}
  
private void createDepthForArray(int currentDepth, Object anArr) {
   Class aClass = anArr.getClass();
   Class componentType = aClass.getComponentType();
   if (componentType == byte.class)
      TestConfig.tab().getRandGen().nextBytes((byte[])anArr);
   int arraySize = Array.getLength(anArr);
   Vector aVec = new Vector();
   aVec.add(componentType.toString());
   for (int i = 0; i < arraySize; i++) {
      Object anObj = getRandomObject(aVec);
      Array.set(anArr, i, anObj);
      createDepth(currentDepth-1, anObj);
   }
}
  
//****************************************************************************
// methods to make random modifications to objects
public boolean modify(Object anObj) {
   Log.getLogWriter().info("RandomValues: Modifying " + TestHelper.toString(anObj));
   int objectDepth = TestConfig.tab().intAt(RandomValuesPrms.objectDepth);
   objectDepth = TestConfig.tab().getRandGen().nextInt(0, objectDepth);
   boolean result = modify(anObj, 0, objectDepth);
   Log.getLogWriter().info("RandomValues, finished attempt at modify, object was modified: " + result +
       ", object is " + TestHelper.toString(anObj));
   return result;
}

boolean modify(Object anObj, int currentDepth, int maxDepth) {
   if (anObj == null) {
      Log.getLogWriter().info("Unable to modify: null");
      return false;
   }
   if (anObj instanceof Map) {
      Map aMap = (Map)anObj;
      if ((currentDepth == maxDepth) || (aMap.size() == 0)) { // make a mod
         randomModify(aMap);
         return true;
      } else { // drill down
         Object key = TestHelper.getRandomKeyInMap(aMap);
         if (TestConfig.tab().getRandGen().nextBoolean()) // drill down key
            return modify(key, currentDepth+1, maxDepth);
         else // drill down value
            return modify(aMap.get(key), currentDepth+1, maxDepth);
      }
   } else if (anObj instanceof List) {
      List aList = (List)anObj;
      if ((currentDepth == maxDepth) || (aList.size() == 0)) { // make a mod
         randomModify(aList);
         return true;
      } else { // drill down
         int randInt = TestConfig.tab().getRandGen().nextInt(1, aList.size() - 1);
         return modify(aList.get(randInt), currentDepth+1, maxDepth);
      }
   } else if (anObj instanceof Set) {
      Set aSet = (Set)anObj;
      if ((currentDepth == maxDepth) || (aSet.size() == 0)) { // make a mod
         randomModify(aSet);
         return true;
      } else { // drill down
         Object element = TestHelper.getRandomElementInSet(aSet);
         return modify(element, currentDepth+1, maxDepth);
      }
   } else if (anObj instanceof BitSet) {
      BitSet bSet = (BitSet)anObj;
      for (int i = 0; i < bSet.length(); i++) {
         if (TestConfig.tab().getRandGen().nextBoolean())
            bSet.set(i);
         else
            bSet.clear(i);
      }
      Log.getLogWriter().info("Modified " + TestHelper.toString(anObj));
      return true;
   } else if (anObj instanceof StringBuffer) {
      StringBuffer aStr = (StringBuffer)anObj;
      Log.getLogWriter().info("Modify: Changing " + TestHelper.toString(aStr));
      int index = TestConfig.tab().getRandGen().nextInt(0, aStr.length());
      StringBuffer xstr = getRandom_StringBuffer();
      aStr.insert(index, xstr);
      int newSize = Math.min(TestConfig.tab().intAt(RandomValuesPrms.stringSize), aStr.length()); 
      aStr.setLength(newSize);
      Log.getLogWriter().info("Modify: After change: " + TestHelper.toString(aStr));
      return true;
   } else if ((anObj instanceof Boolean) || (anObj instanceof Byte) || (anObj instanceof Character) ||
              (anObj instanceof Double) || (anObj instanceof Float) || (anObj instanceof Integer) ||
              (anObj instanceof Long) || (anObj instanceof Short) || (anObj instanceof String) ||
              (anObj instanceof BigDecimal) || (anObj instanceof BigInteger)) {
     // immutable classes
     Log.getLogWriter().info("Unable to modify immutable object : " + TestHelper.toString(anObj));
     return false;
   } else {
      throw new TestException("Do not know how to modify " + anObj.getClass().getName());
   }
}

void randomModify(Map aMap) {
   final int ADD_NEW_KEY = 0;
   final int USE_EXISTING_KEY = 1;
   final int REMOVE = 2;
   int[] choices = (aMap.size() == 0) ? new int[] {ADD_NEW_KEY}
                                      : new int[] {ADD_NEW_KEY, USE_EXISTING_KEY, ADD_NEW_KEY};
   int choice = choices[TestConfig.tab().getRandGen().nextInt(0, choices.length - 1)];
   switch (choice) {
      case ADD_NEW_KEY:
         Object key = getRandomObject(getKeyTypeVec());
         Object value = getRandomObject(getValueTypeVec());
         Log.getLogWriter().info("Modify: Adding new key: " + TestHelper.toString(aMap) + ".put(" + TestHelper.toString(key) 
             + ", " + TestHelper.toString(value));
         aMap.put(key, value);
         break;
      case USE_EXISTING_KEY:
         key = TestHelper.getRandomKeyInMap(aMap);
         value = getRandomObject(getValueTypeVec());
         Log.getLogWriter().info("Modify: Putting existing key: " + TestHelper.toString(aMap) + ".put(" + 
             TestHelper.toString(key) + ", " + TestHelper.toString(value));
         aMap.put(key, value);
         break;
      case REMOVE:
         key = TestHelper.getRandomKeyInMap(aMap);
         Log.getLogWriter().info("Modify: " + TestHelper.toString(aMap) + ".remove(" + TestHelper.toString(key));
         aMap.remove(key);
         break;
      default: 
         throw new TestException("Unknown choice " + choice);
   }
}

void randomModify(List aList) {
   final int ADD = 0;
   final int REMOVE = 2;
   int[] choices = (aList.size() == 0) ? new int[] {ADD}
                                       : new int[] {ADD, REMOVE};
   int choice = choices[TestConfig.tab().getRandGen().nextInt(0, choices.length - 1)];
   switch (choice) {
      case ADD:
         Object element = getRandomObject(getObjectTypeVec());
         Log.getLogWriter().info("Modify: Adding: " + TestHelper.toString(aList) + ".add(" + TestHelper.toString(element) + ")");
         aList.add(element);
         break;
      case REMOVE:
         int index = TestConfig.tab().getRandGen().nextInt(0, aList.size() - 1);
         Log.getLogWriter().info("Modify: " + TestHelper.toString(aList) + ".remove(" + index + ")");
         aList.remove(index);
         break;
      default: 
         throw new TestException("Unknown choice " + choice);
   }
}

void randomModify(Set aSet) {
   final int ADD = 0;
   final int REMOVE = 1;
   int[] choices = (aSet.size() == 0) ? new int[] {ADD}
                                      : new int[] {ADD, REMOVE};
   int choice = choices[TestConfig.tab().getRandGen().nextInt(0, choices.length - 1)];
   switch (choice) {
      case ADD:
         Object element = getRandomObject(getSetElementTypeVec());
         Log.getLogWriter().info("Modify: Adding: " + TestHelper.toString(aSet) + ".add(" + TestHelper.toString(element) + ")"); 
         aSet.add(element);
         break;
      case REMOVE:
         element = TestHelper.getRandomElementInSet(aSet);
         Log.getLogWriter().info("Modify: " + TestHelper.toString(aSet) + ".remove(" + TestHelper.toString(element));
         aSet.remove(element);
         break;
      default: 
         throw new TestException("Unknown choice " + choice);
   }
}

//****************************************************************************
// internal methods
private Object getRandomObject(Vector dataTypeVec) {
   int randInt = TestConfig.tab().getRandGen().nextInt(0, dataTypeVec.size() - 1);
   String objectType = (String)dataTypeVec.elementAt(randInt);
   Object anObj = getRandomObjectForType(objectType);
   return anObj;
}

private Object getRandomObject(Vector dataTypeVec, Object extraObj) {
   int randInt = TestConfig.tab().getRandGen().nextInt(0, dataTypeVec.size() - 1);
   String objectType = (String)dataTypeVec.elementAt(randInt);
   Object anObj = getRandomObjectForType(objectType, extraObj);
   return anObj;
}

/** Get the appropriate objectType vector to use.
 *
 *  @returns A Vector of classes to be used to get a random object.
 */
private Vector getObjectTypeVec() {
   return objectTypeVec;
}

/** Get the appropriate keyType vector to use.
 *
 *  @returns A Vector of classes to be used to get a random key for a dictionary.
 */
private Vector getKeyTypeVec() {
   return keyTypeVec;
}

/** Get the appropriate valueType vector to use.
 *
 *  @returns A Vector of classes to be used to get a random value for a dictionary.
 */
private Vector getValueTypeVec() {
   return valueTypeVec;
}

/** Get the appropriate setElementType vector to use.
 *
 *  @returns A Vector of classes to be used to get a random value for a set.
 */
private Vector getSetElementTypeVec() {
   return setElementVec;
}

/** Return a random String from the given Vector of String.
 *
 *  @returns A String from aVec
 */
private String getRandomStringFromVec(Vector aVec) {
   int randInt = TestConfig.tab().getRandGen().nextInt(0, aVec.size() - 1);
   String result = (String)aVec.elementAt(randInt);
   return result;
}

}
