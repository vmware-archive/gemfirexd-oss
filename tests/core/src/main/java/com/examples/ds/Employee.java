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
package com.examples.ds;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import java.io.*;
import java.util.Date;

public class Employee implements DataSerializable {
  private int id;
  private String name;
  private Date birthday;
  private Company employer;

  public Employee(int id, String name, Date birthday,
                  Company employer){
    this.id = id;
    this.name = name;
    this.birthday = birthday;
    this.employer = employer;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.id);
    out.writeUTF(this.name);
    DataSerializer.writeDate(this.birthday, out);
    DataSerializer.writeObject(this.employer, out);
  }

  public void fromData(DataInput in) 
    throws IOException, ClassNotFoundException {

    this.id = in.readInt();
    this.name = in.readUTF();
    this.birthday = DataSerializer.readDate(in);
    this.employer = (Company) DataSerializer.readObject(in);
  }
}
