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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryObject implements java.io.Serializable, Cloneable, Comparable {
  public String myVersion;

// various queryable fields
public long        aPrimitiveLong;
public Long        aLong;
public int         aPrimitiveInt;
public Integer     anInteger;
public short       aPrimitiveShort;
public Short       aShort;
public float       aPrimitiveFloat;
public Float       aFloat;
public double      aPrimitiveDouble;
public Double      aDouble;
public byte        aPrimitiveByte;
public Byte        aByte;
public char        aPrimitiveChar;
public Character   aCharacter;
public boolean     aPrimitiveBoolean;
public Boolean     aBoolean;
public String      aString;
public byte[]      aByteArray;
public QueryObject aQueryObject;
public Object      extra; // to be used for whatever a test needs this for

public long id; // used for analysis of logs; this id stays the same for the same QueryObject
                // whether it is in a server or client

// ======================================================================
// static fields used to create a new instance of QueryObject

/** All fields (except aByteArray, aQueryObject) have equal values 
 * except when the values are out of range for the field type, for
 * example when a long value is out of range for a byte value.
 * In that case, the desired is cast to the field with less precision.
 */
public static final int EQUAL_VALUES = 1;

/** The fields (except aByteArray, aQueryObject) have sequential values. 
 *  where the ordering of fields is the same order they are declared,
 *  except when the values are out of range for the field type, for
 *  example when the desired value is out of range for a byte value.
 *  In that case, the desired is cast to the field with less precision.
 */
public static final int SEQUENTIAL_VALUES = 2;

/** The fields (except aByteArray, aQueryObject) have random values,
 *  determined by util.RandomValue parameters.
 */
public static final int RANDOM_VALUES = 3;

// ======================================================================
// static fields used to modify a QueryObject with updated values based 
// on another QueryObject

/** Increment fields */
public static final int INCREMENT = 4;

/** Negate fields */
public static final int NEGATE = 5;

/** Null non-primitive fields */
public static final int NULL_NONPRIM_FIELDS = 5;

// ======================================================================
// Constructors and initialization

/** No-arg constructor.
 */
public QueryObject() {
   id = QueryObjectBB.getBB().getSharedCounters().incrementAndRead(QueryObjectBB.QueryObjectID);
   myVersion = "tests/util.QueryObject";
}

/** 
 *  All the query fields are initialized using the given base value
 *  and valueGeneration to determine the values of each field.
 *
 *  @param base The base value for all fields. Fields are all set
 *         to a value using base as appropriate. Unused if 
 *         valueGeneration is RANDOM_VALUES.
 *  @param valueGeneration How to generate the values of the
 *         fields. Can be one of
 *            QueryObject.EQUAL_VALUES
 *            QueryObject.SEQUENTIAL_VALUES
 *            QueryObject.RANDOM_VALUES
 *  @param byteArraySize The size of byte[]. If < 0 then there is
 *         no byteArray (it is null).
 *  @param levels The number of levels of QueryObjects. 1 means
 *         the field aQueryObject is null, 2 means aQueryObject
 *         contains an instance of QueryObject with fields
 *         initialized with base+1, the next level is initialized
 *         with base+2 etc.
 */
public QueryObject(long base, int valueGeneration, int byteArraySize, int levels) {
   id = QueryObjectBB.getBB().getSharedCounters().incrementAndRead(QueryObjectBB.QueryObjectID);
   myVersion = "tests/util.QueryObject";
   this.fillInBaseValues(base, valueGeneration, byteArraySize);
   QueryObject firstQueryObject = null;
   QueryObject currentQueryObject = null;
   for (int i = 1; i < levels; i++) {
      QueryObject queryObject = new QueryObject();
      queryObject.fillInBaseValues(base+i, valueGeneration, byteArraySize);
      if (i == 1) {
         firstQueryObject = queryObject;
         currentQueryObject = queryObject;
      } else {
         currentQueryObject.aQueryObject = queryObject;
         currentQueryObject = queryObject;
      }    
   }
   aQueryObject = firstQueryObject;
}

/** Init fields using base as appropriate, but does not
 *  initialize aQueryObject field.
 *
 *  @param base The base value for all fields. Fields are all set
 *              to a value using base as appropriate.
 *  @param valueGeneration How to generate the values of the
 *         fields. Can be one of
 *            QueryObject.EQUAL_VALUES
 *            QueryObject.SEQUENTIAL_VALUES
 *            QueryObject.RANDOM_VALUES
 *  @param byteArraySize The size of byte[]. 
 */
public void fillInBaseValues(long base, int valueGeneration, int byteArraySize) {
   if (valueGeneration == EQUAL_VALUES) {
      aPrimitiveLong = base;
      aLong = new Long(base);
      aPrimitiveInt = (int)base;
      anInteger = new Integer(aPrimitiveInt);
      aPrimitiveShort = (short)base;
      aShort = new Short(aPrimitiveShort);
      aPrimitiveFloat = base;
      aFloat = new Float(base);
      aPrimitiveDouble = base;
      aDouble = new Double(base);
      aPrimitiveByte = (byte)base;
      aByte = new Byte(aPrimitiveByte);
      aPrimitiveChar = (char)base;
      aCharacter = new Character(aPrimitiveChar);
      aPrimitiveBoolean = true;
      aBoolean = new Boolean(true);
      aString = "" + base;
   } else if (valueGeneration == SEQUENTIAL_VALUES) {
      long currValue = base;
      aPrimitiveLong = currValue++;
      aLong = new Long(currValue++);
      aPrimitiveInt = (int)currValue++;
      anInteger = new Integer((int)(currValue++));
      aPrimitiveShort = (short)(currValue++);
      aShort = new Short((short)(currValue++));
      aPrimitiveFloat = currValue++;
      aFloat = new Float((currValue++));
      aPrimitiveDouble = currValue++;
      aDouble = new Double((currValue++));
      aPrimitiveByte = (byte)(currValue++);
      aByte = new Byte((byte)(currValue++));
      aPrimitiveChar = (char)(currValue++);
      aCharacter = new Character((char)(currValue++));
      aPrimitiveBoolean = true;
      aBoolean = new Boolean(true);
      aString = "" + currValue++;
   } else if (valueGeneration == RANDOM_VALUES) {
      RandomValues rv = new RandomValues();
      aPrimitiveLong = rv.getRandom_long();
      aLong = new Long(rv.getRandom_long());
      aPrimitiveInt = rv.getRandom_int();
      anInteger = new Integer(rv.getRandom_int());
      aPrimitiveShort = rv.getRandom_short();
      aShort = new Short(rv.getRandom_short());
      aPrimitiveFloat = rv.getRandom_float();
      aFloat = new Float(rv.getRandom_float());
      aPrimitiveDouble = rv.getRandom_double();
      aDouble = new Double(rv.getRandom_double());
      aPrimitiveByte = rv.getRandom_byte();
      aByte = new Byte(rv.getRandom_byte());
      aPrimitiveChar = rv.getRandom_char();
      aCharacter = new Character(rv.getRandom_char());
      aPrimitiveBoolean = rv.getRandom_boolean();
      aBoolean = new Boolean(rv.getRandom_boolean());
      aString = rv.getRandom_String();
   }
   if (byteArraySize >= 0) {
      aByteArray = new byte[byteArraySize];
   } 
}

// ======================================================================
// Override inherited methods

/** Return whether this QueryObject is equal to the argument.
 */
public int hashCode() {
   int hashCode = 
      (new Long(aPrimitiveLong)).hashCode() +
      aLong.hashCode() +
      (new Integer(aPrimitiveInt)).hashCode() +
      anInteger.hashCode() +
      (new Short(aPrimitiveShort)).hashCode() +
      aShort.hashCode() +
      (new Float(aPrimitiveFloat)).hashCode() +
      aFloat.hashCode() +
      (new Double(aPrimitiveDouble)).hashCode() +
      aDouble.hashCode() +
      (new Byte(aPrimitiveByte)).hashCode() +
      aByte.hashCode() +
      (new Character(aPrimitiveChar)).hashCode() +
      aCharacter.hashCode() +
      (new Boolean(aPrimitiveBoolean)).hashCode() +
      aBoolean.hashCode() +
      aString.hashCode();
      if (aByteArray != null) {
         hashCode = hashCode + aByteArray.length;
      }
   if (aQueryObject != null) {
      hashCode = hashCode + aQueryObject.hashCode();
   }
   return hashCode;
}
   
/** Return true if the argument is equal to this QueryObject,
 *  false otherwise.
 */
public boolean equals(Object anObj) {
   boolean result = _equals(anObj);
   return result;
}

/** Return true if the argument is equal to this QueryObject,
 *  false otherwise.
 */
private boolean _equals(Object anObj) {
   if (anObj == null) {
       return false;
   }

   if (!(anObj instanceof QueryObject)) {
      return false;
   }

   QueryObject qo = (QueryObject)anObj;
   if (aPrimitiveLong != qo.aPrimitiveLong) {
      return false;
   }
   if (!(aLong.equals(qo.aLong))) {
      return false;
   }
   if (aPrimitiveInt != qo.aPrimitiveInt) {
      return false;
   }
   if (!(anInteger.equals(qo.anInteger))) {
      return false;
   }
   if (aPrimitiveShort != qo.aPrimitiveShort) {
      return false;
   }
   if (!(aShort.equals(qo.aShort))) {
      return false;
   }
   if (aPrimitiveFloat != qo.aPrimitiveFloat) {
      return false;
   }
   if (!(aFloat.equals(qo.aFloat))) {
      return false;
   }
   if (aPrimitiveDouble != qo.aPrimitiveDouble) {
      return false;
   }
   if (!(aDouble.equals(qo.aDouble))) {
      return false;
   }
   if (aPrimitiveByte != qo.aPrimitiveByte) {
      return false;
   }
   if (!(aByte.equals(qo.aByte))) {
      return false;
   }
   if (aPrimitiveChar != qo.aPrimitiveChar) {
      return false;
   }
   if (!(aCharacter.equals(qo.aCharacter))) {
      return false;
   }
   if (aPrimitiveBoolean != qo.aPrimitiveBoolean) {
      return false;
   }
   if (!(aBoolean.equals(qo.aBoolean))) {
      return false;
   }
   if (!(aString.equals(qo.aString))) {
      return false;
   }
   if (this.aByteArray == null) {
      if (qo.aByteArray != null) {
         return false;
      }
   } else { 
      if (qo.aByteArray == null) {
         return false;
      } else {
         if (this.aByteArray.length != qo.aByteArray.length) {
            return false;
         }
      }
   }
   if ((aQueryObject == null) && (qo.aQueryObject != null)) {
      return false;
   }
   if ((aQueryObject != null) && (qo.aQueryObject == null)) {
      return false;
   }
   if (aQueryObject != null) {
      return aQueryObject.equals(qo.aQueryObject);
   }
   return (qo.aQueryObject == null);
}

/** Make a copy of the QueryObject and any QueryObjects referenced
 *  by it, with a different id for all new QueryObjects.
 *  The copy will still be equal to the original.
 */
public Object clone() throws CloneNotSupportedException {
   QueryObject qo = (QueryObject)(super.clone());
   qo.id = QueryObjectBB.getBB().getSharedCounters().incrementAndRead(QueryObjectBB.QueryObjectID);
   if (qo.aQueryObject != null) {
      qo.aQueryObject = (QueryObject)(this.aQueryObject.clone());
   }
   return qo;
}
   
// ======================================================================
// String methods

/** Return a string representation of a QueryObject.
 */
public String toString() {
   return toStringAbbreviated();
}

/** Return a full string representation of a QueryObject, included all levels.
 */
public String toStringFull() {
   return toString(1);
}

/** Recursively return a string representation of a QueryObject.
 *  
 *  @param level The current level of nested QueryObjects.
 */
private String toString(int level) {
   StringBuffer aStr = new StringBuffer();
   for (int i = 1; i < level; i++) {
      aStr.append("==");
   }
   aStr.append(this.getClass().getName() + "(" + System.identityHashCode(this) + ") with id " + id + " (at level " + level + "), ");
   fieldsToString(aStr);
   if (aQueryObject == null) {
      aStr.append("aQueryObject: null");
   } else {
      aStr.append("aQueryObject:\n" + aQueryObject.toString(level+1));
   }
   aStr.append("\n");
   return aStr.toString();
}

/** Return a string showing the given QueryObject constant.
 */
public String constantToString(int constant) {
   if (constant == EQUAL_VALUES) {
      return ("EQUAL_VALUES");
   } else if (constant == SEQUENTIAL_VALUES) {
      return ("SEQUENTIAL_VALUES");
   } else if (constant == RANDOM_VALUES) {
      return ("RANDOM_VALUES");
   } else if (constant == INCREMENT) {
      return ("INCREMENT");
   } else if (constant == NEGATE) {
      return ("NEGATE");
   } else {
      throw new TestException("Test problem; Unknown constant " + constant);
   }
}

/** Return a String showing the given list of QueryObjects in 
 *  abbreviated form.
 *
 *  @param aList A List of QueryObjects.
 */
public static String toString(List aList) {
   return toString(aList, false);
}

/** Return a String showing the given list of QueryObjects in 
 *  full form.
 *
 *  @param aList A List of QueryObjects.
 */
public static String toStringFull(List aList) {
   return toString(aList, false);
}

/** Return a String showing the given list of QueryObjects in an
 *  abbreviated form.
 *
 *  @param aList A List of QueryObjects.
 */
public static String toStringAbbreviated(List aList) {
   return toString(aList, true);
}

/** Return a String showing the given list of QueryObjects.
 *
 *  @param aList A List of QueryObjects.
 *  @param abbreviated If true, then return an abbreviated string for
 *         each QueryObject, otherwise return a full string.
 */
private static String toString(List aList, boolean abbreviated) {
   StringBuffer aStr = new StringBuffer();
   aStr.append("List of size " + aList.size() + "\n");
   for (int i = 0; i < aList.size(); i++) {
      Object anObj = aList.get(i);
      if (anObj instanceof QueryObject) {
         QueryObject qo = (QueryObject)(anObj);
         if (abbreviated) {
            aStr.append(qo.toStringAbbreviated());
         } else {
            aStr.append(qo.toStringFull());
         }
      } else {
         aStr.append(anObj);
      }
      aStr.append("\n");
   }
   return aStr.toString();
}

/** Return a String showing the given Map of QueryObjects in 
 *  abbreviated form.
 *
 *  @param aMap A Map of QueryObjects as values.
 */
public static String toString(Map aMap) {
   return toString(aMap, false);
}

/** Return a String showing the given Map of QueryObjects in 
 *  full form.
 *
 *  @param aMap A Map of QueryObjects as values.
 */
public static String toStringFull(Map aMap) {
   return toString(aMap, false);
}

/** Return a String showing the given Map of QueryObjects in an
 *  abbreviated form.
 *
 *  @param aMap A Map of QueryObjects as values.
 */
public static String toStringAbbreviated(Map aMap) {
   return toString(aMap, true);
}

/** Return a String showing the given Map of QueryObjects.
 *
 *  @param aMap A Map of QueryObjects as values.
 *  @param abbreviated If true, then return an abbreviated string for
 *         each QueryObject, otherwise return a full string.
 */
private static String toString(Map aMap, boolean abbreviated) {
   StringBuffer aStr = new StringBuffer();
   aStr.append("Map of size " + aMap.size() + "\n");
   Iterator it = aMap.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next(); 
      Object anObj = aMap.get(key);
      aStr.append(key + "=");
      if (anObj instanceof QueryObject) {
         QueryObject qo = (QueryObject)(anObj);
         if (abbreviated) {
            aStr.append(qo.toStringAbbreviated());
         } else {
            aStr.append(qo.toStringFull());
         }
      } else {
         aStr.append(anObj);
      }
      aStr.append("\n");
   }
   return aStr.toString();
}

/** Return an abbreviated string representation of a QueryObject.
 */
public String toStringAbbreviated() {
   QueryObject currentQO = this;
   int levels = 0;
   while (currentQO != null) {
      levels++;
      currentQO = currentQO.aQueryObject;
   }
   return (this.getClass().getName() + " with id " + this.id + " (contains " + levels + " levels, aPrimitiveLong=" + aPrimitiveLong + ")");
}

/** Return a one-level string representation of a QueryObject without
 *  printing out any nested QueryObjects.
 */
public String toStringOneLevel() {
   StringBuffer aStr = new StringBuffer();
   aStr.append(this.getClass().getName() + " with id " + id + ", ");
   fieldsToString(aStr);
   aStr.append("aQueryObject: " + aQueryObject);
   return aStr.toString();
}

/** Append to the given StringBuffer the basic fields in this
 *  QueryObject.
 */
protected void fieldsToString(StringBuffer aStr) {
   final int limit = 50; // limit of how many bytes of string to represent
   aStr.append("myVersion: " + myVersion + ", ");
   aStr.append("aPrimitiveLong: " + aPrimitiveLong + ", ");
   aStr.append("aLong: " + aLong + ", ");
   aStr.append("aPrimitiveInt: " + aPrimitiveInt + ", ");
   aStr.append("anInteger: " + anInteger + ", ");
   aStr.append("aPrimitiveShort: " + aPrimitiveShort + ", ");
   aStr.append("aShort: " + aShort + ", ");
   aStr.append("aPrimitiveFloat: " + aPrimitiveFloat + ", ");
   aStr.append("aFloat: " + aFloat + ", ");
   aStr.append("aPrimitiveDouble: " + aPrimitiveDouble + ", ");
   aStr.append("aDouble: " + aDouble + ", ");
   aStr.append("aPrimitiveByte: " + aPrimitiveByte + ", ");
   aStr.append("aByte: " + aByte + ", ");
   aStr.append("aPrimitiveChar: " + (byte)(aPrimitiveChar) + ", ");
   aStr.append("aCharacter: " + (byte)(aCharacter.charValue()) + "(byte value), ");
   aStr.append("aPrimitiveBoolean: " + aPrimitiveBoolean + ", ");
   aStr.append("aBoolean: " + aBoolean + ", ");
   if (aString.length() <= limit) {
      aStr.append("String: \""  + aString + "\", ");
   } else {
      aStr.append("<String of length " + aString.length() + " starting with \"" + aString.substring(0, limit) + "\">, ");
   }
   if (aByteArray == null) {
      aStr.append("byte[]: " + aByteArray + ", ");
   } else {
      aStr.append("byte[] of size: " + aByteArray.length + ", ");
   }
   aStr.append("extra: " + extra + " ");
}

// ======================================================================
// Methods to modify QueryObjects

/** Modify the QueryObject according to changeValueGeneration and delta.
 *
 *  @param changeValueGeneration How to determine the new values 
 *         Can be one of:
 *         QueryObject.INCREMENT
 *         QueryObject.NEGATE 
 *         QueryObject.NULL_NONPRIM_FIELDS 
 *         If any fields are null and this is INCREMENT or NEGATE then
 *         the field is filled in with a random value.
 *  @param delta The number to increment or decrement; unused if
 *         changeValueGeneration is QueryObject.NEGATE.
 *  @param log If true, then log what is being modified, if false don't
 *         log since output can be large.
 */
public void modify(int changeValueGeneration, int delta, boolean log) {
   if (log) {
      Log.getLogWriter().info("Modifying " + this.toStringFull() + " with " + constantToString(changeValueGeneration) + " delta " + delta);
   }
   RandomValues rv = new RandomValues();
   if (aLong == null)      aLong = new Long(rv.getRandom_long());
   if (anInteger == null)  anInteger = new Integer(rv.getRandom_int());
   if (aShort == null)     aShort = new Short(rv.getRandom_short());
   if (aFloat == null)     aFloat = new Float(rv.getRandom_float());
   if (aDouble == null)    aDouble = new Double(rv.getRandom_double());
   if (aByte == null)      aByte = new Byte(rv.getRandom_byte());
   if (aCharacter == null) aCharacter = new Character(rv.getRandom_char());
   if (aBoolean == null)   aBoolean = new Boolean(rv.getRandom_boolean());
   if (aString == null)    aString = rv.getRandom_String();
   if (changeValueGeneration == INCREMENT) {
      aPrimitiveLong = aPrimitiveLong + delta;
      aLong = new Long(aLong.longValue() + delta);

      aPrimitiveInt = aPrimitiveInt + delta;
      anInteger = new Integer(anInteger.intValue() + delta);

      aPrimitiveShort = (short)(aPrimitiveShort + delta);
      aShort = new Short((short)(aShort.shortValue() + delta));

      aPrimitiveFloat = aPrimitiveFloat + delta;
      aFloat = new Float(aFloat.floatValue() + delta);

      aPrimitiveDouble = aPrimitiveDouble + delta;
      aDouble = new Double(aDouble.doubleValue() + delta);

      aPrimitiveByte = (byte)(aPrimitiveByte + delta);
      aByte = new Byte((byte)(aByte.byteValue() + delta));

      aPrimitiveChar = (char)(((byte)aPrimitiveChar) + delta);
      aCharacter = new Character((char)(((byte)(aCharacter.charValue())) + delta));

      aPrimitiveBoolean = !aPrimitiveBoolean;
      aBoolean = new Boolean(!(aBoolean.booleanValue()));

      try {
         aString = "" + (Long.valueOf(aString).longValue() + delta);
      } catch (NumberFormatException e) { // must be a random string
         aString = rv.getRandom_String();
      }
   } else if (changeValueGeneration == NEGATE) {
      aPrimitiveLong = -aPrimitiveLong;
      aLong = new Long(-aLong.longValue());

      aPrimitiveInt = -aPrimitiveInt;
      anInteger = new Integer(-anInteger.intValue());

      aPrimitiveShort = (short)(-aPrimitiveShort);
      aShort = new Short((short)(-aShort.shortValue()));

      aPrimitiveFloat = -aPrimitiveFloat;
      aFloat = new Float(-aFloat.floatValue());

      aPrimitiveDouble = -aPrimitiveDouble;
      aDouble = new Double(-aDouble.doubleValue());

      aPrimitiveByte = (byte)(-aPrimitiveByte);
      aByte = new Byte((byte)(-aByte.byteValue()));

      aPrimitiveChar = (char)(-((byte)aPrimitiveChar));
      aCharacter = new Character((char)(-((byte)(aCharacter.charValue()))));

      aPrimitiveBoolean = !aPrimitiveBoolean;
      aBoolean = new Boolean(!(aBoolean.booleanValue()));

      try {
         aString = new String("" + -(Long.valueOf(aString).longValue()));
      } catch (NumberFormatException e) { // must be a random string
         aString = rv.getRandom_String();
      }
   } else if (changeValueGeneration == NULL_NONPRIM_FIELDS) {
      aLong = null;
      anInteger = null;
      aShort = null;
      aFloat = null;
      aDouble = null;
      aByte = null;
      aCharacter = null;
      aBoolean = null;
      aString = null;
   } else {
      throw new TestException("unknown changeValueGeneration " + changeValueGeneration);
   }
   if (log) {
      Log.getLogWriter().info("Done modifying " + this.toStringFull() + " with " + constantToString(changeValueGeneration) + " delta is " + delta);
   }
}

/** Return a new instance of QueryObject. The returned object's fields
 *  are the receiver's field values modified as specified by the change
 *  argument. The new instance will have aByteArray of the same size
 *  as the receiver's byteArray, and any references to another QueryObject
 *  via the aQueryObject field is also set to a new instance, with its
 *  fields set according to the changeValueGeneration argument.
 *
 *  @param changeValueGeneration How to determine the values of new
 *         instance. Can be one of :
 *         QueryObject.INCREMENT
 *         QueryObject.NEGATE 
 *  @param delta The number to increment or decrement; unused if
 *         changeValueGeneration is QueryObject.NEGATE.
 *  @param log If true, then log what is being modified, if false don't
 *         log since output can be large.
 */
public QueryObject modifyWithNewInstance(int changeValueGeneration, int delta, boolean log) {
   Log.getLogWriter().info("Creating a new copy (to modify) of " + this.toStringAbbreviated());
   try {
      QueryObject qo = (QueryObject)(this.clone());
      Log.getLogWriter().info("Created " + qo.toStringAbbreviated() + ", which is a copy of " + 
          this.toStringAbbreviated());
      qo.modify(changeValueGeneration, delta, log);
      Log.getLogWriter().info("Created modified copy: " + qo.toStringFull());
      return qo;
   } catch (CloneNotSupportedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Modifies the QueryObject at the given depth by changing its values
 *  according to changeValueGeneration and delta. The values are directly
 *  modified without creating any new QueryObject instances.
 * 
 *  @param depth Level number of the QueryObject, to be used according
 *         to other arguments.
 *  @param changeValueGeneration How to determine the values of the
 *         modified QueryObject. Can be one of :
 *         QueryObject.INCREMENT
 *         QueryObject.NEGATE 
 *  @param delta The number to increment or decrement; unused if
 *         changeValueGeneration is QueryObject.NEGATE.
 *  @param randomDepth If true then modify a QueryObject at a randomly
 *         chosen depth between 1 and depth, if false then modify the
 *         QueryObject at the given depth.
 *  @param log If true, then log what is being modified, if false don't
 *         log since output can be large.
 *         
 */
public void modify(int depth, int changeValueGeneration, int delta, boolean randomDepth, boolean log) {
   int modifyDepth = depth;
   if (randomDepth) {
      modifyDepth = (new java.util.Random()).nextInt(depth) + 1;
   }
   if (log) {
      Log.getLogWriter().info("Modifying " + this.toStringFull() + " at depth " + modifyDepth);
   }
   QueryObject qo = this.getAtDepth(modifyDepth);
   qo.modify(changeValueGeneration, delta, log);
}

/** Creates new instance of the root QueryObject and modifies the values of
 *  the QueryObject at the given depth by changing its values according to 
 *  changeValueGeneration and delta. In other words, the only new instance
 *  will be the top level QueryObject, but a deepter level QueryObject might
 *  be modified and it will not be a new instance. Since the top level QueryObject
 *  is a new instance, it could be used in a put to update a region entry.
 * 
 *  @param changeValueGeneration How to determine the values of the
 *         modified QueryObject. Can be one of :
 *         QueryObject.INCREMENT
 *         QueryObject.NEGATE 
 *  @param delta The number to increment or decrement; unused if
 *         changeValueGeneration is QueryObject.NEGATE.
 *  @param randomDepth If true then modify a QueryObject at a randomly
 *         chosen depth between 1 and depth, if false then modify the
 *         QueryObject at the given depth.
 *  @param log If true, then log what is being modified, if false don't
 *         log since output can be large.
 *
 *  @returns The new QueryObject instance.
 */
public QueryObject modifyWithNewInstance(int depth, int changeValueGeneration, int delta, boolean randomDepth, boolean log) {
   try {
      QueryObject qo = (QueryObject)(this.clone());
      Log.getLogWriter().info("Created " + qo.toStringAbbreviated() + ", which is a copy of " + 
          this.toStringAbbreviated());
      qo.modify(depth, changeValueGeneration, delta, randomDepth, log);
      Log.getLogWriter().info("Created modified copy: " + qo.toStringFull());
      return qo;
   } catch (CloneNotSupportedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

// ======================================================================
// Other general use methods

/** Return the depth of the QueryObject. 
 *
 *  @returns The depth of the QueryObject. For example, if 
 *           this.aQueryObject is null, then return 1.
 */
public int depth() {
   int depth = 1;
   QueryObject currentQO = this;
   while (currentQO.aQueryObject != null) {
      depth++;
      currentQO = currentQO.aQueryObject;
   }
   return depth;
}

/** Return the QueryObject at the given depth. For example, if depth is 
 *  1, then return this. If depth is 3, then return the QueryObject that 
 *  is qo.aQueryObject.aQueryObject.
 *
 *  @param depth The desired depth of the QueryObject.
 *  @return The QueryObject at the given depth or null if the QueryObject
 *          is not deep enough for depth.
 */
public QueryObject getAtDepth(int depth) {
   QueryObject currentQO = this;
   for (int i = 2; i <= depth; i++) {
      if (currentQO == null) {
         return null;
      }
      currentQO = currentQO.aQueryObject;
   }
   return currentQO;
}

/** Return true if the two QueryObjects have the same id, false otherwise.
 */
public boolean hasEqualId(QueryObject anObj) {
   return this.id == anObj.id;
}

// ======================================================================
// Methods to implement comparable interface
public int compareTo(Object anObj) {
   QueryObject qo = (QueryObject)anObj;
   if (this.id < qo.id) {
      return -1;
   } else if (this.id == qo.id) {
      return 0;
   } else {
      return 1;
   }
}

// ======================================================================
// Main to test QueryObject methods

/** Test QueryObject methods
 */
public static void main(String[] args) throws Exception {
   // print to see what the output looks like
   QueryObject qo1 = new QueryObject(1, EQUAL_VALUES, 4, 3);
   System.out.println("qo1 is " + qo1 + "\n");
   QueryObject qo2 = new QueryObject(1, SEQUENTIAL_VALUES, 4, 3);
   System.out.println("qo2 is " + qo2 + "\n");

   QueryObject qo3 = new QueryObject(4113, SEQUENTIAL_VALUES, 0, 3);
   QueryObject qo4 = new QueryObject(4113, SEQUENTIAL_VALUES, 0, 6);

   System.out.println("qo3 equals qo4: " + qo3.equals(qo4));
   System.out.println("qo4 equals qo3: " + qo4.equals(qo3));
   System.out.println("qo3 hashcode: " + qo3.hashCode());
   System.out.println("qo4 hashcode: " + qo4.hashCode());
}

}
