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
/**
 * 
 */
package util;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;
import pdx.PdxPrms;

/**
 * @author lynn
 *
 */
public class VersionedQueryObject extends QueryObject {

  public enum Day {
    Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday
  }

  //pdx fields for this version
  public long        pdx_aPrimitiveLong;
  public Long        pdx_aLong;
  public int         pdx_aPrimitiveInt;
  public Integer     pdx_anInteger;
  public short       pdx_aPrimitiveShort;
  public Short       pdx_aShort;
  public float       pdx_aPrimitiveFloat;
  public Float       pdx_aFloat;
  public double      pdx_aPrimitiveDouble;
  public Double      pdx_aDouble;
  public byte        pdx_aPrimitiveByte;
  public Byte        pdx_aByte;
  public char        pdx_aPrimitiveChar;
  public Character   pdx_aCharacter;
  public boolean     pdx_aPrimitiveBoolean;
  public Boolean     pdx_aBoolean;
  public String      pdx_aString;
  public Day         pdx_aDay;
  
  static transient List dayList;
  
  static {
    Log.getLogWriter().info("In static initializer for testsVersions/version2/objects.VersionedPortfolio");
    dayList = new ArrayList();
    dayList.addAll(EnumSet.allOf(Day.class));
  }

  /**
   * 
   */
  public VersionedQueryObject() {
    myVersion = "testsVersions/version2/util.VersionedQueryObject";
  }

  /**
   * @param base
   * @param valueGeneration
   * @param byteArraySize
   * @param levels
   */
  public VersionedQueryObject(long base, int valueGeneration,
      int byteArraySize, int levels) {
    super(base, valueGeneration, byteArraySize, levels);
    myVersion = "testsVersions/version2/util.VersionedQueryObject";
  }
  
  /** Append to the given StringBuffer the basic fields in this
   *  QueryObject.
   */
  @Override
  protected void fieldsToString(StringBuffer aStr) {
     final int limit = 50; // limit of how many bytes of string to represent
     super.fieldsToString(aStr);
     aStr.append("pdx_aPrimitiveLong: " + pdx_aPrimitiveLong + ", ");
     aStr.append("pdx_aLong: " + pdx_aLong + ", ");
     aStr.append("pdx_aPrimitiveInt: " + pdx_aPrimitiveInt + ", ");
     aStr.append("pdx_anInteger: " + pdx_anInteger + ", ");
     aStr.append("pdx_aPrimitiveShort: " + pdx_aPrimitiveShort + ", ");
     aStr.append("pdx_aShort: " + pdx_aShort + ", ");
     aStr.append("pdx_aPrimitiveFloat: " + pdx_aPrimitiveFloat + ", ");
     aStr.append("pdx_aFloat: " + pdx_aFloat + ", ");
     aStr.append("pdx_aPrimitiveDouble: " + pdx_aPrimitiveDouble + ", ");
     aStr.append("pdx_aDouble: " + pdx_aDouble + ", ");
     aStr.append("pdx_aPrimitiveByte: " + pdx_aPrimitiveByte + ", ");
     aStr.append("pdx_aByte: " + pdx_aByte + ", ");
     aStr.append("pdx_aPrimitiveChar: " + (byte)(pdx_aPrimitiveChar) + ", ");
     if (pdx_aCharacter == null) {
       aStr.append("pdx_aCharacter: " + pdx_aCharacter + ", ");
     } else {
      aStr.append("pdx_aCharacter: " + (byte)(pdx_aCharacter.charValue()) + "(byte value), ");
     }
     aStr.append("pdx_aPrimitiveBoolean: " + pdx_aPrimitiveBoolean + ", ");
     aStr.append("pdx_aBoolean: " + pdx_aBoolean + ", ");
     if (aString.length() <= limit) {
        aStr.append("pdx_String: \""  + pdx_aString + "\", ");
     } else {
        aStr.append("<String of length " + pdx_aString.length() + " starting with \"" + pdx_aString.substring(0, limit) + "\">, ");
     }
     aStr.append("pdx_aDay: " + pdx_aDay + ", ");
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
  @Override
  public void fillInBaseValues(long base, int valueGeneration, int byteArraySize) {
     super.fillInBaseValues(base, valueGeneration, byteArraySize);
     
     // make the pdx fields mirror their non-pdx counterpart
     pdx_aPrimitiveLong = aPrimitiveLong;
     pdx_aLong = new Long(aLong);
     pdx_aPrimitiveInt = aPrimitiveInt;
     pdx_anInteger = new Integer(anInteger);
     pdx_aPrimitiveShort = aPrimitiveShort;
     pdx_aShort = new Short(aShort);;
     pdx_aPrimitiveFloat = aPrimitiveFloat;
     pdx_aFloat = new Float(aFloat);
     pdx_aPrimitiveDouble = aDouble;
     pdx_aDouble = new Double(aDouble);
     pdx_aPrimitiveByte = aPrimitiveByte;
     pdx_aByte = new Byte(aByte);;
     pdx_aPrimitiveChar = aPrimitiveChar;
     pdx_aCharacter = new Character(aCharacter);
     pdx_aPrimitiveBoolean = aPrimitiveBoolean;
     pdx_aBoolean = new Boolean(aBoolean);
     pdx_aString = new String(aString);
     pdx_aDay = getDayForBase(base);
  }

  public void myToData(PdxWriter writer) {
    Log.getLogWriter().info("In testsVersions/version2/util.VersionedQueryObject.toData: " + this +
        (PdxPrms.getLogStackTrace() ? ("\n" + TestHelper.getStackTrace()) : ""));
    writer.writeString("myVersion", myVersion);
    writer.writeLong("aPrimitiveLong", aPrimitiveLong);
    writer.writeLong("aLong", aLong);
    writer.writeInt("aPrimitiveInt", aPrimitiveInt);
    writer.writeInt("anInteger", anInteger);
    writer.writeShort("aPrimitiveShort", aPrimitiveShort);
    writer.writeShort("aShort", aShort);
    writer.writeFloat("aPrimitiveFloat", aPrimitiveFloat);
    writer.writeFloat("aFloat", aFloat);
    writer.writeDouble("aPrimitiveDouble", aPrimitiveDouble);
    writer.writeDouble("aDouble", aDouble);
    writer.writeByte("aPrimitiveByte", aPrimitiveByte);
    writer.writeByte("aByte", aByte);
    writer.writeChar("aPrimitiveChar", aPrimitiveChar);
    writer.writeChar("aCharacter", aCharacter);
    writer.writeBoolean("aPrimitiveBoolean", aPrimitiveBoolean);
    writer.writeBoolean("aBoolean", aBoolean);
    writer.writeString("aString", aString);
    writer.writeByteArray("aByteArray", aByteArray);
    writer.writeObject("aQueryObject", aQueryObject);
    writer.writeObject("extra", extra);
    writer.writeLong("id", id);

    writer.writeLong("pdx_aPrimitiveLong", pdx_aPrimitiveLong);
    writer.writeLong("pdx_aLong", pdx_aLong);
    writer.writeInt("pdx_aPrimitiveInt", pdx_aPrimitiveInt);
    writer.writeInt("pdx_anInteger", pdx_anInteger);
    writer.writeShort("pdx_aPrimitiveShort", pdx_aPrimitiveShort);
    writer.writeShort("pdx_aShort", pdx_aShort);
    writer.writeFloat("pdx_aPrimitiveFloat", pdx_aPrimitiveFloat);
    writer.writeFloat("pdx_aFloat", pdx_aFloat);
    writer.writeDouble("pdx_aPrimitiveDouble", pdx_aPrimitiveDouble);
    writer.writeDouble("pdx_aDouble", pdx_aDouble);
    writer.writeByte("pdx_aPrimitiveByte", pdx_aPrimitiveByte);
    writer.writeByte("pdx_aByte", pdx_aByte);
    writer.writeChar("pdx_aPrimitiveChar", pdx_aPrimitiveChar);
    writer.writeChar("pdx_aCharacter", pdx_aCharacter);
    writer.writeBoolean("pdx_aPrimitiveBoolean", pdx_aPrimitiveBoolean);
    writer.writeBoolean("pdx_aBoolean", pdx_aBoolean);
    writer.writeString("pdx_aString", pdx_aString);
    writer.writeObject("pdx_aDay", pdx_aDay);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void myFromData(PdxReader reader) {
    myVersion = reader.readString("myVersion");
    Log.getLogWriter().info("In testsVersions/version2/util.VersionedQueryObject.fromData with myVersion: " + 
        myVersion);
    aPrimitiveLong = reader.readLong("aPrimitiveLong");
    aLong = reader.readLong("aLong");
    aPrimitiveInt = reader.readInt("aPrimitiveInt");
    anInteger = reader.readInt("anInteger");
    aPrimitiveShort = reader.readShort("aPrimitiveShort");
    aShort = reader.readShort("aShort");
    aPrimitiveFloat = reader.readFloat("aPrimitiveFloat");
    aFloat = reader.readFloat("aFloat");
    aPrimitiveDouble = reader.readDouble("aPrimitiveDouble");
    aDouble = reader.readDouble("aDouble");
    aPrimitiveByte = reader.readByte("aPrimitiveByte");
    aByte = reader.readByte("aByte");
    aPrimitiveChar = reader.readChar("aPrimitiveChar");
    aCharacter = reader.readChar("aCharacter");
    aPrimitiveBoolean = reader.readBoolean("aPrimitiveBoolean");
    aBoolean = reader.readBoolean("aBoolean");
    aString = reader.readString("aString");
    aByteArray = reader.readByteArray("aByteArray");
    aQueryObject = (QueryObject)reader.readObject("aQueryObject");
    extra = reader.readObject("extra");
    id = reader.readLong("id");

    pdx_aPrimitiveLong = reader.readLong("pdx_aPrimitiveLong");
    pdx_aLong = reader.readLong("pdx_aLong");
    pdx_aPrimitiveInt = reader.readInt("pdx_aPrimitiveInt");
    pdx_anInteger = reader.readInt("pdx_anInteger");
    pdx_aPrimitiveShort = reader.readShort("pdx_aPrimitiveShort");
    pdx_aShort = reader.readShort("pdx_aShort");
    pdx_aPrimitiveFloat = reader.readFloat("pdx_aPrimitiveFloat");
    pdx_aFloat = reader.readFloat("pdx_aFloat");
    pdx_aPrimitiveDouble = reader.readDouble("pdx_aPrimitiveDouble");
    pdx_aDouble = reader.readDouble("pdx_aDouble");
    pdx_aPrimitiveByte = reader.readByte("pdx_aPrimitiveByte");
    pdx_aByte = reader.readByte("pdx_aByte");
    pdx_aPrimitiveChar = reader.readChar("pdx_aPrimitiveChar");
    pdx_aCharacter = reader.readChar("pdx_aCharacter");
    pdx_aPrimitiveBoolean = reader.readBoolean("pdx_aPrimitiveBoolean");
    pdx_aBoolean = reader.readBoolean("pdx_aBoolean");
    pdx_aString = reader.readString("pdx_aString");
    pdx_aDay = (Day)(reader.readObject("pdx_aDay"));

    Log.getLogWriter().info("After reading fields in fromData: " + this +
        (PdxPrms.getLogStackTrace() ? ("\n" + TestHelper.getStackTrace()) : ""));
  }
  
  /** Return the appropriate enum Day value given the base
   * 
   * @param base Index to base the Day calculation on.
   * @return A value of Day.
   */
  public static Day getDayForBase(long base) {
      Day aDay = (Day)(dayList.get((int)(base % dayList.size())));
      return aDay;
  }
}
