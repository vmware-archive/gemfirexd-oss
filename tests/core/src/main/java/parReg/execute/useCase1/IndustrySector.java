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
package parReg.execute.useCase1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;

public class IndustrySector implements DataSerializable {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(IndustrySector.class, (byte)13) {
      public DataSerializable newInstance() {
        return new IndustrySector();
      }
    });
  }

  public int id_sector;

  public int id_parent;

  public String nm_sector;

  public String tx_sector;

  public IndustrySector() {
  }

  public IndustrySector(int id_sector, int id_parent, String nm_sector,
      String tx_sector) {
    this.id_sector = id_sector;
    this.id_parent = id_parent;
    this.nm_sector = nm_sector;
    this.tx_sector = tx_sector;
  }

  public String toString() {
    return "|" + id_sector + "|" + nm_sector + "|" + tx_sector + "|";
  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    id_sector = input.readInt();
    id_parent = input.readInt();
    nm_sector = DataSerializer.readString(input);
    tx_sector = DataSerializer.readString(input);
  }

  public void toData(DataOutput output) throws IOException {
    output.writeInt(id_sector);
    output.writeInt(id_parent);
    DataSerializer.writeString(nm_sector, output);
    DataSerializer.writeString(tx_sector, output);
  }
}
