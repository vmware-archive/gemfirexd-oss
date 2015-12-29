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
/** Subclass of util.ValueHolder that implements DataSerializable
 * 
 */
package util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

import com.gemstone.gemfire.DataSerializable;

/**
 * @author lynn
 *
 */
public class VHDataSerializable extends BaseValueHolder implements DataSerializable {

  /**
   * @param anObj
   * @param randomValues
   */
  public VHDataSerializable(Object anObj, RandomValues randomValues) {
    super(anObj, randomValues);
    myVersion = "util.VHDataSerializable";
  }

  /**
   * @param randomValues
   */
  public VHDataSerializable(RandomValues randomValues) {
    super(randomValues);
    myVersion = "util.VHDataSerializable";
  }

  /**
   * @param nameFactoryName
   * @param randomValues
   */
  public VHDataSerializable(String nameFactoryName, RandomValues randomValues) {
    super(nameFactoryName, randomValues);
    myVersion = "util.VHDataSerializable";
  }

  /**
   * @param anObj
   * @param randomValues
   * @param initModVal
   */
  public VHDataSerializable(Object anObj, RandomValues randomValues,
      Integer initModVal) {
    super(anObj, randomValues, initModVal);
    myVersion = "util.VHDataSerializable";
  }

  /**
   * @param nameFactoryName
   * @param randomValues
   * @param initModVal
   */
  public VHDataSerializable(String nameFactoryName, RandomValues randomValues,
      Integer initModVal) {
    super(nameFactoryName, randomValues, initModVal);
    myVersion = "util.VHDataSerializable";
  }

  /**
   * 
   */
  public VHDataSerializable() {
    super();
    myVersion = "util.VHDataSerializable";
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializable#toData(java.io.DataOutput)
   */
  public void toData(DataOutput out) throws IOException {
    // write myValue field
    myToData(this, out);
  }

  public static void myToData(BaseValueHolder vh, DataOutput out) throws IOException {
    out.writeUTF(vh.myVersion);
    writeObject(out, vh.myValue);
    writeObject(out, vh.extraObject);
    if (vh.modVal == null) {
      out.writeUTF("null");
    } else {
      out.writeUTF(vh.modVal.toString());
    }
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializable#fromData(java.io.DataInput)
   */
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    myFromData(this, in);
  }

  public static void myFromData(BaseValueHolder vh, DataInput in) throws IOException {
    vh.myVersion = in.readUTF();
    vh.myValue = readObject(in);
    vh.extraObject = readObject(in);
    // read modVal
    String aStr = in.readUTF();
    if (aStr.equals("null")) {
      vh.modVal = null;
    } else {
      vh.modVal = Integer.valueOf(aStr);
    }
  }

  /** Convert an object to a byte array
   * 
   * @param anObj The object to convert.
   * @return The byte array that represents anObj
   */
  protected static byte[] toBytes(Object anObj) {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(); 
    ObjectOutput out;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(anObj); 
      byte[] byteArr = bos.toByteArray();   
      out.close(); 
      bos.close();  
      return byteArr;
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  
  /** Given a DataOutput, write the bytes of anObj to it.
   * 
   * @param out The stream to write bytes to.
   * @param anObj The object to convert to bytes and write to out.
   */
  private static void writeObject(DataOutput out, Object anObj) {
    byte[] byteArr = toBytes(anObj);
    try {
      out.writeInt(byteArr.length);
      out.write(byteArr);
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  
  /** Read bytes from the given DataInput and convert to an Object
   * 
   * @param in The stream to read bytes from.
   * @return The Object represented by the bytes.
   */
  private static Object readObject(DataInput in) {
    try {
      int length = in.readInt();
      byte[] byteArr = new byte[length];
      in.readFully(byteArr);
      ByteArrayInputStream bis = new ByteArrayInputStream(byteArr); 
      ObjectInput objIn = new ObjectInputStream(bis); 
      Object anObj = objIn.readObject();
      bis.close();
      objIn.close(); 
      return anObj;
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

}
