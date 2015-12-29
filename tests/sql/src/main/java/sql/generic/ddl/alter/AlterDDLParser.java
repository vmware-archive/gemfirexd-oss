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
import sql.generic.ddl.DDLAction;
import sql.generic.ddl.Executor;
import util.TestException;

public class AlterDDLParser {
  
  String ddl;
  Executor executor;
  public AlterDDLParser (String ddl, Executor executor ) 
  {
    this.ddl = ddl;
    this.executor = executor;
  }
  
  public Operation parseDDL() {
    Log.getLogWriter().info("DDL before parsing :: " + this.ddl);
    Operation operation = getDDLOperation();
    return operation.parse();      
  }
  
  public Operation getDDLOperation (){
    switch (AlterKeywordParser.getKeyWordOperation(ddl)) {
      case PRIMARYKEY:   
          return new AlterPrimaryKeyOperation(ddl,executor, getAction());
      case FOREIGNKEY:
          return new AlterForeignKeyOperation(ddl,executor, getAction());
      case UNIQ:
          return new AlterUniqueOperation(ddl,executor, getAction());
      case CHECK:
          return new AlterCheckOperation(ddl,executor, getAction());
      case CONSTRAINT:
          return new AlterConstraintOperation(ddl,executor, getAction());
      case COLUMN:
        return new AlterColumnOperation(ddl,executor, getAction());
      case EVICTION:
         return new AlterEvictionOperation(ddl,executor, getAction());
      case LISTENER:
         return new AlterAsyncEventListenerOperation(ddl,executor, getAction());         
      case GATEWAYSENDER:
          return new AlterGatewaySenderOperation(ddl,executor, getAction());
      default: throw new TestException (" Unsupported Alter syntax: " + ddl);
    }
    
  }
  
  public DDLAction getAction(){    
      if ( ddl.contains(DDLAction.ADD.name())) {
        return DDLAction.ADD;
      }
      else if ( ddl.contains(DDLAction.SET.name()) || ddl.contains(" ALTER ")) {
        return DDLAction.SET;
      }
      else if ( ddl.contains(DDLAction.DROP.name())) {
        return DDLAction.DROP;
      } else  throw new TestException (" Unsupported Alter syntax: " + ddl);
  }
}
