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

public class FxRate implements DataSerializable {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(FxRate.class, (byte)10) {
      public DataSerializable newInstance() {
        return new FxRate();
      }
    });
  }

  public String id_ccy_std; // Instrument.id_ccy_main

  public int id_region;

  public String id_ccy_bse;

  public float rt_fx;

  public FxRate() {
  }

  public FxRate(String id, int id_region, String id_ccy_bse, float rt_fx) {
    this.id_ccy_std = id;
    this.id_region = id_region;
    this.id_ccy_bse = id_ccy_bse;
    this.rt_fx = rt_fx;
  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    id_ccy_std = DataSerializer.readString(input);
    id_region = input.readInt();
    id_ccy_bse = DataSerializer.readString(input);
    rt_fx = input.readFloat();
  }

  public void toData(DataOutput output) throws IOException {
    DataSerializer.writeString(id_ccy_std, output);
    ;
    output.writeInt(id_region);
    DataSerializer.writeString(id_ccy_bse, output);
    output.writeFloat(id_region);
  }

  public String toString() {
    return "|" + id_ccy_std + "|" + id_region + "|" + id_ccy_bse + "|" + rt_fx
        + "|";
  }
}
