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

import com.gemstone.gemfire.DataSerializable;

public class MyObjectDataSerializable extends MyObject implements DataSerializable {
  public MyObjectDataSerializable() {
  }

  public MyObjectDataSerializable(long number, String s) {
    super(number, s);
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeLong(f1);
    out.writeUTF(f2);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    f1 = in.readLong();
    f2 = in.readUTF();
  }
}