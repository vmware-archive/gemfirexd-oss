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

import com.gemstone.gemfire.DataSerializer;

import hydra.TestConfig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

/**
 * @author lynn
 *
 */
public class VHDataSerializer extends DataSerializer {

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializer#getSupportedClasses()
   */
  @Override
  public Class<?>[] getSupportedClasses() {
    return new Class[] { BaseValueHolder.class };
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializer#toData(java.lang.Object, java.io.DataOutput)
   */
  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    if (!(o instanceof BaseValueHolder)) {
      return false;
    }
    BaseValueHolder vh = (BaseValueHolder)o;

    // write myValue field
    out.writeUTF(vh.myValue.getClass().getName());
    if (vh.myValue instanceof Long) {
      out.writeLong((Long)vh.myValue);
    } else if (vh.myValue instanceof String) {
      out.writeUTF((String)vh.myValue);
    } else if (vh.myValue instanceof Integer) {
      out.writeInt((Integer)vh.myValue);
    } else if (vh.myValue instanceof BigInteger) {
      byte[] theBytes = ((BigInteger)vh.myValue).toByteArray();
      out.writeInt(theBytes.length);
      out.write(theBytes);
    } else {
      throw new TestException("Unknown myValue class " + vh.myValue.getClass().getName());
    }

    // write extraObject; for current test usage, this is always a byte[]
    if ((vh.extraObject.getClass().isArray()) && (vh.extraObject.getClass().getComponentType() == byte.class)) {
      byte[] byteArr = (byte[])vh.extraObject;
      out.writeInt(byteArr.length);
      out.write((byte[])vh.extraObject);
    } else {
      throw new TestException("Test does not support extraObject of class " + vh.extraObject.getClass().getName());
    }

    // write modVal
    if (vh.modVal == null) {
      out.writeUTF("null");
    } else {
      out.writeUTF(vh.modVal.toString());
    }
    return true;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializer#fromData(java.io.DataInput)
   */
  @Override
  public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
    BaseValueHolder vh = new ValueHolder();

    // read myValue field
    String className = in.readUTF();
    if (className.equals(Long.class.getName())) {
      vh.myValue = in.readLong();
    } else if (className.equals(String.class.getName())) {
      vh.myValue = in.readUTF();
    } else if (className.equals(Integer.class.getName())) {
      vh.myValue = in.readInt();
    } else if (className.equals(BigInteger.class.getName())) {
      int numBytes = in.readInt();
      byte[] theBytes = new byte[numBytes];
      in.readFully(theBytes);
      vh.myValue = new BigInteger(theBytes);
    } else {
      throw new TestException("Unknown class name " + className);
    }

    // read extraObject; for current test usage, this is always a byte[]
    int numBytes = in.readInt();
    byte[] theBytes = new byte[numBytes];
    in.readFully(theBytes);
    vh.extraObject = theBytes;

    // read modVal
    String aStr = in.readUTF();
    if (aStr.equals("null")) {
      vh.modVal = null;
    } else {
      vh.modVal = Integer.valueOf(aStr);
    }
    return vh;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.DataSerializer#getId()
   */
  @Override
  public int getId() {
    return 11223344;
  }

}
