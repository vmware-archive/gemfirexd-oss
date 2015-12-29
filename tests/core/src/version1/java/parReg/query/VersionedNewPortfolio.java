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
/**
 * 
 */
package parReg.query;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxWriter;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.EnumSet;

import hydra.Log;

import pdx.PdxPrms;

/**
 * @author lynn
 *
 */
public class VersionedNewPortfolio extends NewPortfolio {

  public VersionedNewPortfolio() {
    myVersion = "testsVersions/version1/parReg.query.VersionedNewPortfolio";
  }
  
  public VersionedNewPortfolio(String name, int id) {
    super(name, id);
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName() + " [myVersion=" + this.myVersion + ", id="
        + this.id + ", name=" + this.name + ", status=" + this.status
        + ", type=" + this.type + ", positions=" + this.positions
        + ", undefinedTestField=" + this.undefinedTestField;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  public void myToData(PdxWriter writer) {
    Log.getLogWriter().info("In testsVersions/version1/parReg.query.VersionedNewPortfolio.myToData: " + this);
    writer.writeString("myVersion", myVersion);
    writer.writeInt("id", id);
    writer.writeString("name", name);
    writer.writeString("status", status);
    writer.writeString("type", type);
    writer.writeObject("positions", positions);
    writer.writeString("undefinedTestField", undefinedTestField);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void myFromData(PdxReader reader) {
    myVersion = reader.readString("myVersion");
    Log.getLogWriter().info("In testsVersions/version1/parReg.query.VersionedNewPortfolio.myFromData with myVersion: " + myVersion);
    id = reader.readInt("id");
    name = reader.readString("name");
    status = reader.readString("status");
    type = reader.readString("type");
    positions = (Map)reader.readObject("positions");
    undefinedTestField = reader.readString("undefinedTestField");
    Log.getLogWriter().info("After reading fields in fromData: " + this);
  }

}
