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

public class UndRiskSensitivity extends ImntRiskSensitivity {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(UndRiskSensitivity.class, (byte)19) {
      public DataSerializable newInstance() {
        return new UndRiskSensitivity();
      }
    });
  }

  public int id_imnt;

  public int id_imnt_und; // PositionUndRisk.d_imnt_und

  public String id_param_rsk;

  public float am_exp_rsk;

  public String id_ccy_unit;

  public UndRiskSensitivity() {
  }

  public UndRiskSensitivity(int id_imnt, int id_imnt_und, String id_param_rsk,
      float am_exp_rsk, String id_ccy_unit) {
    this.id_imnt = id_imnt;
    this.id_imnt_und = id_imnt_und;
    this.id_param_rsk = id_param_rsk;
    this.am_exp_rsk = am_exp_rsk;
    this.id_ccy_unit = id_ccy_unit;
  }

  public String toString() {
    return "|" + id_imnt + "|" + id_imnt_und + "|" + id_param_rsk + "|"
        + am_exp_rsk + "|" + id_ccy_unit + "|";
  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    super.fromData(input);
    id_imnt = input.readInt();
    id_imnt_und = input.readInt();
    id_param_rsk = DataSerializer.readString(input);
    am_exp_rsk = input.readFloat();
    id_ccy_unit = DataSerializer.readString(input);
  }

  public void toData(DataOutput output) throws IOException {
    super.toData(output);
    output.writeInt(id_imnt);
    output.writeInt(id_imnt_und);
    DataSerializer.writeString(id_param_rsk, output);
    output.writeFloat(am_exp_rsk);
    DataSerializer.writeString(id_ccy_unit, output);
  }
}
