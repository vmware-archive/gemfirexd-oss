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

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;
import pdx.PdxPrms;

/**
 * @author lynn
 *
 */
public class VersionedQueryObject extends QueryObject {

  public VersionedQueryObject() {
    myVersion = "testsVersions/version1/util.VersionedQueryObject";
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
    myVersion = "testsVersions/version1/util.VersionedQueryObject";
  }
  
  public void myToData(PdxWriter out) {
    Log.getLogWriter().info("In testsVersions/version1/util.VersionedQueryObject.toData: " + this +
        (PdxPrms.getLogStackTrace() ? ("\n" + TestHelper.getStackTrace()) : ""));
    out.writeString("myVersion", myVersion);
    out.writeLong("aPrimitiveLong", aPrimitiveLong);
    out.writeLong("aLong", aLong);
    out.writeInt("aPrimitiveInt", aPrimitiveInt);
    out.writeInt("anInteger", anInteger);
    out.writeShort("aPrimitiveShort", aPrimitiveShort);
    out.writeShort("aShort", aShort);
    out.writeFloat("aPrimitiveFloat", aPrimitiveFloat);
    out.writeFloat("aFloat", aFloat);
    out.writeDouble("aPrimitiveDouble", aPrimitiveDouble);
    out.writeDouble("aDouble", aDouble);
    out.writeByte("aPrimitiveByte", aPrimitiveByte);
    out.writeByte("aByte", aByte);
    out.writeChar("aPrimitiveChar", aPrimitiveChar);
    out.writeChar("aCharacter", aCharacter);
    out.writeBoolean("aPrimitiveBoolean", aPrimitiveBoolean);
    out.writeBoolean("aBoolean", aBoolean);
    out.writeString("aString", aString);
    out.writeByteArray("aByteArray", aByteArray);
    out.writeObject("aQueryObject", aQueryObject);
    out.writeObject("extra", extra);
    out.writeLong("id", id);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void myFromData(PdxReader in) {
    myVersion = in.readString("myVersion");
    Log.getLogWriter().info("In testsVersions/version1/util.VersionedQueryObject.fromData with myVersion: " + 
        myVersion);
    aPrimitiveLong = in.readLong("aPrimitiveLong");
    aLong = in.readLong("aLong");
    aPrimitiveInt = in.readInt("aPrimitiveInt");
    anInteger = in.readInt("anInteger");
    aPrimitiveShort = in.readShort("aPrimitiveShort");
    aShort = in.readShort("aShort");
    aPrimitiveFloat = in.readFloat("aPrimitiveFloat");
    aFloat = in.readFloat("aFloat");
    aPrimitiveDouble = in.readDouble("aPrimitiveDouble");
    aDouble = in.readDouble("aDouble");
    aPrimitiveByte = in.readByte("aPrimitiveByte");
    aByte = in.readByte("aByte");
    aPrimitiveChar = in.readChar("aPrimitiveChar");
    aCharacter = in.readChar("aCharacter");
    aPrimitiveBoolean = in.readBoolean("aPrimitiveBoolean");
    aBoolean = in.readBoolean("aBoolean");
    aString = in.readString("aString");
    aByteArray = in.readByteArray("aByteArray");
    aQueryObject = (QueryObject)in.readObject("aQueryObject");
    extra = in.readObject("extra");
    id = in.readLong("id");
    Log.getLogWriter().info("After reading fields in fromData: " + this +
        (PdxPrms.getLogStackTrace() ? ("\n" + TestHelper.getStackTrace()) : ""));
  }


}
