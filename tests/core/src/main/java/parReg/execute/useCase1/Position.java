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

public class Position implements DataSerializable {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(Position.class, (byte)15) {
      public DataSerializable newInstance() {
        return new Position();
      }
    });
  }

  public int id_posn_new; // PositionRisk.id_posn_new,

  // PositionUndRisk.id_posn_new

  public int id_imnt;

  public int id_book; // Hierarchy.id_book

  public float am_posn;

  public Position() {
  }

  public Position(int id_posn_new, int id_imnt, int id_book, float am_posn) {
    this.id_posn_new = id_posn_new;
    this.id_imnt = id_imnt;
    this.id_book = id_book;
    this.am_posn = am_posn;
  }

  public String toString() {
    return "|" + id_posn_new + "|" + id_imnt + "|" + id_book + "|" + am_posn
        + "|";
  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    id_posn_new = input.readInt();
    id_imnt = input.readInt();
    id_book = input.readInt();
    am_posn = input.readFloat();
    ;
  }

  public void toData(DataOutput output) throws IOException {
    output.writeInt(id_posn_new);
    output.writeInt(id_imnt);
    output.writeInt(id_book);
    output.writeFloat(am_posn);
  }

}
