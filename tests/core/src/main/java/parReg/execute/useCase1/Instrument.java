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

public class Instrument implements DataSerializable {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(Instrument.class, (byte)14) {
      public DataSerializable newInstance() {
        return new Instrument();
      }
    });
  }

  public int id_imnt; // Position.id_imnt, RiskSensitivity.id_imnt,

  // RiskSensitivity.id_imnt_und

  public String nm_imnt;

  public String id_typ_imnt;

  public String id_ccy_main;

  public float am_sz_ctrt;

  public int id_sector; // Industry.id_sector

  public String id_ctry_issuer;

  public Instrument(int id_imnt, String nm_imnt, String id_typ_imnt,
      String id_ccy_main, float am_sz_ctrt, int id_sector, String id_ctry_issuer) {
    this.id_imnt = id_imnt;
    this.nm_imnt = nm_imnt;
    this.id_typ_imnt = id_typ_imnt;
    this.id_ccy_main = id_ccy_main;
    this.am_sz_ctrt = am_sz_ctrt;
    this.id_sector = id_sector;
  }

  public String toString() {
    return "|" + id_imnt + "|" + nm_imnt + "|" + id_typ_imnt + "|"
        + id_ccy_main + "|" + am_sz_ctrt + "|" + id_sector + "|"
        + id_ctry_issuer + "|";
  }

  public Instrument() {

  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    id_imnt = input.readInt();
    nm_imnt = DataSerializer.readString(input);
    id_typ_imnt = DataSerializer.readString(input);
    id_ccy_main = DataSerializer.readString(input);
    am_sz_ctrt = input.readFloat();
    id_sector = input.readInt();
    id_ctry_issuer = DataSerializer.readString(input);
  }

  public void toData(DataOutput output) throws IOException {
    output.writeInt(id_imnt);
    DataSerializer.writeString(nm_imnt, output);
    DataSerializer.writeString(id_typ_imnt, output);
    DataSerializer.writeString(id_ccy_main, output);
    output.writeFloat(am_sz_ctrt);
    output.writeInt(id_sector);
    DataSerializer.writeString(id_ctry_issuer, output);
  }

}
