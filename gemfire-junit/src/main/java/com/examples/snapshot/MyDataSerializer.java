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
package com.examples.snapshot;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;

public class MyDataSerializer extends DataSerializer {
  @Override
  public Class<?>[] getSupportedClasses() {
    return new Class[] { MyObjectDataSerializable2.class};
  }

  @Override
  public boolean toData(Object o, DataOutput out) throws IOException {
    MyObject obj = (MyObject) o;
    out.writeLong(obj.f1);
    out.writeUTF(obj.f2);
    
    return true;
  }

  @Override
  public Object fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    MyObjectDataSerializable2 obj = new MyObjectDataSerializable2();
    obj.f1 = in.readLong();
    obj.f2 = in.readUTF();
    
    return obj;
  }

  @Override
  public int getId() {
    return 8892;
  }
  
  public static class MyObjectDataSerializable2 extends MyObject {
    public MyObjectDataSerializable2() {
    }

    public MyObjectDataSerializable2(long number, String s) {
      super(number, s);
    }
  }
}
