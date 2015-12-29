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
package sql.sqlutil;

import sql.ddlStatements.*;
import util.TestException;

/**
 * @author eshu
 *
 */
public class DDLStmtsFactory {
  public static final int PROCEDURE = 1;
  public static final int AUTHORIZATION = 2;
  public static final int FUNCTION =3;
  public static final int DAP = 4;
  
  public DDLStmtIF createDDLStmt(int whichDDL) {
    DDLStmtIF ddlStmtIF;
    switch (whichDDL) {
    case PROCEDURE:
      ddlStmtIF = new ProcedureDDLStmt();
      break;
    case AUTHORIZATION:
      ddlStmtIF = new AuthorizationDDLStmt();
      break;
    case FUNCTION:
    	ddlStmtIF = new FunctionDDLStmt();
    	break;
    case DAP:
    	ddlStmtIF = new DAPDDLStmt();
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
    
    return whichDDL;
  }
  
}
