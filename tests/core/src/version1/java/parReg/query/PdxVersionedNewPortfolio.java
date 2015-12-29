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
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.Log;

import java.io.IOException;
import java.util.Map;

import pdx.PdxPrms;

/**
 * @author lynn
 *
 */
public class PdxVersionedNewPortfolio extends VersionedNewPortfolio implements
    PdxSerializable {
  
  public PdxVersionedNewPortfolio() {
    myVersion = "testsVersions/version1/parReg.query.PdxVersionedNewPortfolio";
  }
  
  public PdxVersionedNewPortfolio(String name, int id) {
    super(name, id);
    myVersion = "testsVersions/version1/parReg.query.PdxVersionedNewPortfolio";
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#toData(com.gemstone.gemfire.pdx.PdxWriter)
   */
  public void toData(PdxWriter writer) {
    if (PdxPrms.getLogToAndFromData()) {
      Log.getLogWriter().info("In testsVersions/version1/parReg.query.PdxVersionedNewPortfolio.toData, calling myToData...");
    }
    myToData(writer);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializable#fromData(com.gemstone.gemfire.pdx.PdxReader)
   */
  public void fromData(PdxReader reader) {
    myVersion = reader.readString("myVersion");
    if (PdxPrms.getLogToAndFromData()) {
      Log.getLogWriter().info("In testsVersions/version1/parReg.query.PdxVersionedNewPortfolio.fromData, calling myFromData...");
    }
    myFromData(reader);
  }

}
