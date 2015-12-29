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
import java.util.Random;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;

public class ImntRiskSensitivity implements DataSerializable {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator
        .register(new Instantiator(ImntRiskSensitivity.class, (byte)12) {
          public DataSerializable newInstance() {
            return new ImntRiskSensitivity();
          }
        });
  }

  public int id_imnt;

  public String id_param_rsk; // PsotionRisk.id_param_rsk,

  // PositionUndRisk.id_param_rsk

  public float am_exp_rsk;

  public String id_ccy_unit;

  public ImntRiskSensitivity(int id_imnt, String id_param_rsk,
      float am_exp_rsk, String id_ccy_unit) {
    this.id_imnt = id_imnt;
    this.id_param_rsk = id_param_rsk;
    this.am_exp_rsk = am_exp_rsk;
    this.id_ccy_unit = id_ccy_unit;
  }

  public ImntRiskSensitivity() {
  }

  public String toString() {
    return "|" + id_imnt + "|" + id_param_rsk + "|" + am_exp_rsk + "|"
        + id_ccy_unit + "|";
  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    id_imnt = input.readInt();
    id_param_rsk = DataSerializer.readString(input);
    am_exp_rsk = input.readFloat();
    id_ccy_unit = DataSerializer.readString(input);
  }

  public void toData(DataOutput output) throws IOException {
    output.writeInt(id_imnt);
    DataSerializer.writeString(id_param_rsk, output);
    output.writeFloat(am_exp_rsk);
    DataSerializer.writeString(id_ccy_unit, output);
  }

}
