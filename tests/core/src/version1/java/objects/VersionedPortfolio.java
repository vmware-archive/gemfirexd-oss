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
package objects;

import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;

import java.io.IOException;

/**
 * @author lynn
 *
 */
public class VersionedPortfolio extends Portfolio {

  public VersionedPortfolio() {
    myVersion = "testsVersions/version1/objects.VersionedPortfolio";
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  public void myToData(PdxWriter writer) {
    Log.getLogWriter().info("In testsVersions/version1/objects.VersionedPortfolio.myToData: " + this);
    writer.writeString("myVersion", myVersion);
    writer.writeInt("ID", ID);
    writer.writeString("type", type);
    writer.writeString("status", status);
    writer.writeByteArray("payload", payload);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void myFromData(PdxReader reader) {
    myVersion = reader.readString("myVersion");
    Log.getLogWriter().info("In testsVersions/version1/objects.VersionedPortfolio.myFromData with myVersion: " + myVersion);
    ID = reader.readInt("ID");
    type = reader.readString("type");
    status = reader.readString("status");
    payload = reader.readByteArray("payload");
    Log.getLogWriter().info("After reading fields in fromData: " + this);
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.getClass().getName()  + " (toString from version1) [myVersion=" + this.myVersion + ", ID="
        + this.ID + ", type=" + this.type + ", status=" + this.status
        + ", payload=" + this.payload + "]";
  }

}
