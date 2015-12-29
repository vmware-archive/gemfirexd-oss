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
package org.apache.ddlutils.task;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.model.Database;
import org.apache.tools.ant.BuildException;

import com.pivotal.gemfirexd.callbacks.EventErrorFileToDBWriter;
/**
 * Command to replay failed events  
 * @author sjigyasu
 */
public class ReplayFailedDMLsCommand extends Command {

  private String errorFileName;
  
  @Override
  public void execute(DatabaseTaskBase task, Database model)
      throws BuildException {
    Connection conn = null;
    try {
      BasicDataSource ds = task.getDataSource();
      conn = ds.getConnection();
      EventErrorFileToDBWriter.writeXMLToDB(conn, errorFileName);
    } catch (Exception e) {
      throw new BuildException(e);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // Ignore the exception 
        }
      }
    }
  }

  @Override
  public boolean isRequiringModel() {
    return false;
  }

  public void setErrorFileName(String name) {
    errorFileName = name;
  }
  
  public String getErrorFileName() {
    return errorFileName;
  }
}
