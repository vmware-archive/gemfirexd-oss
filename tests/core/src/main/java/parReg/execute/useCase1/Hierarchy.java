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

/**
 * id_region,nm_region,id_ccy_rpt,id_own_bus,id_desk_purp,
 * id_rsk_bus,id_bank_perm,id_rsk_region,nm_rsk_region,
 * id_desk,nm_desk,id_country,nm_country,id_country_ccy_rpt,
 * id_country_bank_perm,id_prtf,nm_prtf,id_cl_prtf,id_ccy_std,
 * id_prtf_bank_perm,id_book,nm_book,id_le,id_exchange_key,id_fsa_appr
 * 
 * 1,London,EUR,2,,34,N,1,London,10000136,COUNTRY_FUNDS_SWAPS
 * ,1706,TEST,EUR,N,10012688,TEST3,N,EUR,N,10063706,TEST3,634,10000009,N
 * 1,London,EUR,6,EDG,6,N,1,London,10000025,PPB_LONDON
 * ,1108,PPB_RELATIVE_VALUE,EUR,N,10006239,PPB_RV_SL,N,ITL,N,10056180,PPB_RV_SL,637,10000009,N
 */

public class Hierarchy implements DataSerializable {
  private static final long serialVersionUID = 1L;

  static {
    Instantiator.register(new Instantiator(Hierarchy.class, (byte)11) {
      public DataSerializable newInstance() {
        return new Hierarchy();
      }
    });
  }

  // 1,London,EUR,6,EDG,6,N,
  // 1,London,10000025,PPB_LONDON ,
  // 1108,PPB_RELATIVE_VALUE,EUR,N,
  // 10006239,PPB_RV_SL,N,ITL,N,
  // 10056180,PPB_RV_SL,637,10000009,N

  public int id_book; // Instrument.id_book

  public String nm_book;

  public int id_prtf;

  public String nm_prtf;

  public int id_country;

  public String nm_country;

  public int id_desk;

  public String nm_desk;

  public int id_region;

  public String nm_region;

  public Hierarchy() {
  }

  public Hierarchy(int id_book, String nm_book, int id_prtf, String nm_prtf,
      int id_country, String nm_country, int id_desk, String nm_desk,
      int id_region, String nm_region) {
    this.id_book = id_book;
    this.nm_book = nm_book;
    this.id_prtf = id_prtf;
    this.nm_prtf = nm_prtf;
    this.id_country = id_country;
    this.nm_country = nm_country;
    this.id_desk = id_desk;
    this.nm_desk = nm_desk;
    this.id_region = id_region;
    this.nm_region = nm_region;
  }

  public void fromData(DataInput input) throws IOException,
      ClassNotFoundException {
    id_region = input.readInt();
    nm_region = DataSerializer.readString(input);
    id_desk = input.readInt();
    nm_desk = DataSerializer.readString(input);

    id_country = input.readInt();
    nm_country = DataSerializer.readString(input);

    id_prtf = input.readInt();
    nm_prtf = DataSerializer.readString(input);

    id_book = input.readInt();
    nm_book = DataSerializer.readString(input);

  }

  public void toData(DataOutput output) throws IOException {
    output.writeInt(id_region);
    DataSerializer.writeString(nm_region, output);

    output.writeInt(id_desk);
    DataSerializer.writeString(nm_desk, output);

    output.writeInt(id_country);
    DataSerializer.writeString(nm_country, output);

    output.writeInt(id_prtf);
    DataSerializer.writeString(nm_prtf, output);

    output.writeInt(id_book);
    DataSerializer.writeString(nm_book, output);

  }

  public String toString() {
    return "|" + id_book + "|" + nm_book + "|" + "+|" + id_prtf + "|" + nm_prtf
        + "|" + id_country + "|" + nm_country + "|" + id_desk + "|" + nm_desk
        + "|" + id_region + "|" + nm_region;
  }

}
