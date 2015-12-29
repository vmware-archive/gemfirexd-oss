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
package sql.generic.ddl.create;

import hydra.Log;
import sql.generic.SQLGenericPrms;
import sql.generic.ddl.Index.GenericIndex;
import util.TestException;
import util.TestHelper;

public class DDLStmtFactory {
  public static final int PROCEDURE = 1;

  public static final int AUTHORIZATION = 2;

  public static final int FUNCTION = 3;

  public static final int DAP = 4;

  public static final int INDEX = 5;

  public DDLStmtIF createDDLStmt(int whichDDL) {
    DDLStmtIF ddlStmtIF = null;
    switch (whichDDL) {
      case PROCEDURE:
      case DAP:
        String prefix = "sql.generic.ddl.procedures.";
        String procedureName = SQLGenericPrms.getAnyProcedure().trim();
        int loc = SQLGenericPrms.getProcedureNames().indexOf(procedureName);
        ddlStmtIF = loadClass(prefix + procedureName);
        break;
      case AUTHORIZATION:
        break;
      case FUNCTION:
        String prefixFunction = "sql.generic.ddl.Functions.";
        String functionName = SQLGenericPrms.getAnyFunction().trim();
        loc = SQLGenericPrms.getFunctionNames().indexOf(functionName);
        ddlStmtIF = loadClass(prefixFunction + functionName);
        break;
      case INDEX:
        ddlStmtIF = new GenericIndex(SQLGenericPrms.getAnyIndexGroup());
        break;
      default:
        throw new TestException("Unknown operation " + whichDDL);
    }
    return ddlStmtIF;
  }

  public static int getInt(String ddl) {
    int whichDDL = -1;
    if (ddl.equalsIgnoreCase("procedure"))
      whichDDL = PROCEDURE;
    else if (ddl.equalsIgnoreCase("authorization"))
      whichDDL = AUTHORIZATION;
    else if (ddl.equalsIgnoreCase("function"))
      whichDDL = FUNCTION;
    else if (ddl.equalsIgnoreCase("dap"))
      whichDDL = DAP;
    else if (ddl.equalsIgnoreCase("index"))
      whichDDL = INDEX;

    return whichDDL;
  }

  public DDLStmtIF loadClass(String fullyQualifiedClassName) {
    DDLStmtIF loadedClass = null;
    try {
      Class proc = Class.forName(fullyQualifiedClassName);
      loadedClass = (DDLStmtIF)proc.newInstance();
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IllegalAccessException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (InstantiationException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    return loadedClass;
  }

}
