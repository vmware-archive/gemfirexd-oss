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
package sql.datagen;

import java.io.File;
import java.rmi.RemoteException;
import java.sql.Connection;

import util.TestException;
import hydra.FileUtil;
import hydra.Log;
import hydra.RemoteTestModule;

/**
 * 
 * @author Rahul Diyewar
 */

public class DataGeneratorClient {

  public void parseMapper(String mapper, Connection conn) {
    if (DataGeneratorBB.getBB().getSharedCounters()
        .incrementAndRead(DataGeneratorBB.PARSE_MAPPER) == 1) {
      File mfile = new File(mapper);
      String destMapper = null;
      if (mfile.exists()) {
        destMapper = "mapper.prop";
        FileUtil.copyFile(mapper, destMapper);
      } else {
        Log.getLogWriter().warning("Mapper file does not Exists: " + mapper);
      }

      String classname = DataGeneratorMgr.class.getName();
      String methodname = "parseMapperFile";

      Class[] types = new Class[1];
      types[0] = String.class;

      Object[] args = new Object[1];
      args[0] = destMapper;
      try {
        RemoteTestModule.Master.invoke(classname, methodname, types, args);
      } catch (RemoteException e) {
        throw new TestException("In parsing mapper :" + mapper, e);
      }
    } else {
      Log.getLogWriter().warning("Mapper is already parsed " + mapper);
    }
  }
  
  public void generateTableCSVs(String[] tables, int[] rowCounts){
    
  }
}
