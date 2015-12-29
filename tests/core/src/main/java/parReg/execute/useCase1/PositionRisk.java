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
import com.gemstone.gemfire.Instantiator;


public class PositionRisk implements DataSerializable {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(PositionRisk.class, (byte)16) {
      public DataSerializable newInstance() {
        return new PositionRisk();
      }
    });
  }

  public int id_posn_new;

  // public Date dt_effective;
  public int id_imnt;

  public float am_posn;

  public float am_fv;

  public int am_delta;

  public int am_delta_avg;

  public int am_delta_equity;

  public int am_delta_ccy;

  public int am_vega;

  public int am_gamma;

  public int id_book;

  public int id_prtf;

  public int id_country;

  public int id_desk;

  public PositionRisk() {
  }

  public PositionRisk(int id_posn_new, int id_imnt, float am_posn, float am_fv) {
    this.id_posn_new = id_posn_new;
    this.id_imnt = id_imnt;
    this.am_posn = am_posn;
    this.am_fv = am_fv;
  }

  public String toString() {
    return "|" + id_posn_new + "|" + id_imnt + "|" + am_posn + "|" + am_fv
        + "|";
  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    id_posn_new = input.readInt();
    // dt_effective = DataSerializer.readDate(input);
    id_imnt = input.readInt();
    am_posn = input.readFloat();
    am_fv = input.readFloat();
  }

  public void toData(DataOutput output) throws IOException {
    output.writeInt(id_posn_new);
    // DataSerializer.writeDate(dt_effective, output);
    output.writeInt(id_imnt);
    output.writeFloat(am_posn);
    output.writeFloat(am_fv);
  }

  public String asBookString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("position id=").append(this.id_posn_new).append("; am_fv=")
        .append(this.am_fv).append("; am_theta=");
    return buffer.toString();
  }

}
