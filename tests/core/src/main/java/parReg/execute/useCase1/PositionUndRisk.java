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

public class PositionUndRisk implements DataSerializable {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(PositionUndRisk.class, (byte)17) {
      public DataSerializable newInstance() {
        return new PositionUndRisk();
      }
    });
  }

  public int id_posn_new;

  public int id_imnt;

  public int id_imnt_und;

  public float am_risk;

  public String id_param_rsk;

  public int id_desk;

  public int id_country;

  public int id_prtf;

  public int id_sector;

  public PositionUndRisk() {
  }

  public String toString() {
    return "|" + id_posn_new + "|" + id_imnt + "|" + id_imnt_und + "|"
        + am_risk + "|" + id_param_rsk + "|" + id_desk + "|" + id_country + "|"
        + id_prtf + "|" + id_sector;
  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    id_posn_new = input.readInt();
    id_imnt = input.readInt();
    id_imnt = input.readInt();
    id_param_rsk = DataSerializer.readString(input);
  }

  public void toData(DataOutput output) throws IOException {
    output.writeInt(id_posn_new);
    output.writeInt(id_imnt);
    output.writeInt(id_imnt_und);
    DataSerializer.writeString(id_param_rsk, output);
  }
}
