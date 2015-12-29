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
package portableDataExchange.serializer;

import java.util.Map;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.PdxWriter;

/**
 * An example of a PdxSerializer that can serialize PortfolioPdx objects.
 * 
 * @author GemStone Systems, Inc.
 * @since 6.6 
 */
public class PortfolioSerializer implements PdxSerializer {

  @Override
  public Object fromData(Class<?> clazz, PdxReader reader) {
    if (!clazz.equals(PortfolioPdx.class)) {
      return null;
    }
    
    PortfolioPdx instance = new PortfolioPdx();
    instance.id = reader.readInt("id");

    boolean isIdentity =  reader.isIdentityField("id");

    if (isIdentity == false) {
      throw new IllegalStateException("Portfolio id is identity field");
    }

    boolean isId = reader.hasField("id");

    if (isId == false) {
      throw new IllegalStateException("Portfolio id field not found");
    }

    boolean isNotId = reader.hasField("ID");

    if (isNotId == true) {
      throw new IllegalStateException("Portfolio isNotId field found");
    }

    instance.pkid = reader.readString("pkid");
    instance.position1 = (PositionPdx)reader.readObject("position1");
    instance.position2 = (PositionPdx)reader.readObject("position2");
    instance.positions = (Map<String, PositionPdx>) reader.readObject("positions");
    instance.type = reader.readString("type");
    instance.status = reader.readString("status");
    instance.names = reader.readStringArray("names");
    instance.newVal = reader.readByteArray("newVal");
    instance.creationDate = reader.readDate("creationDate");
    instance.arrayNull = reader.readByteArray("arrayNull");
    instance.arrayZeroSize = reader.readByteArray("arrayZeroSize");
    
    return instance; 
  }

  @Override
  public boolean toData(Object o, PdxWriter writer) {
    if (!(o instanceof PortfolioPdx)) {
      return false;
    }
    
    PortfolioPdx instance = (PortfolioPdx) o;
    writer.writeInt("id", instance.id)
        .markIdentityField("id") //identity field
        .writeString("pkid", instance.pkid)
        .writeObject("position1", instance.position1)
        .writeObject("position2", instance.position2)
        .writeObject("positions", instance.positions)
        .writeString("type", instance.type)
        .writeString("status", instance.status)
        .writeStringArray("names", instance.names)
        .writeByteArray("newVal", instance.newVal)
        .writeDate("creationDate", instance.creationDate)
        .writeByteArray("arrayNull", instance.arrayNull)
        .writeByteArray("arrayZeroSize", instance.arrayZeroSize);
    
    return true;
  }
}
