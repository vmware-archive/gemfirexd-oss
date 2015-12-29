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
package sql.generic.ddl.alter;

import hydra.Log;

import java.sql.Connection;
import java.sql.SQLException;

import sql.generic.ddl.Executor;

public class GenericAlterDDL {

  String ddl = "";

  Connection dConn, gConn;

  boolean verificationAgainstDerby = false;

  SQLException derbyException = null, gfxdException = null;

  public GenericAlterDDL(String ddl, Connection dConn, Connection gConn) {
    this.ddl = ddl;
    this.dConn = dConn;
    this.gConn = gConn;
    if (dConn != null) {
      verificationAgainstDerby = true;
    }
  }

  public GenericAlterDDL(String ddl, Connection gConn) {
    this.ddl = ddl;
    this.dConn = null;
    this.gConn = gConn;
    verificationAgainstDerby = false;
  }

  public void executeDDL() {
    Operation operation = new AlterDDLParser(ddl, new Executor(dConn, gConn))
        .parseDDL();
    operation.execute();
  }

}
