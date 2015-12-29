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
package sql.alterTable;
import java.util.Vector;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.TestConfig;

public class AlterTablePrms  extends BasePrms {

 static {
   setValues( AlterTablePrms.class );
 }

 /*
  * (HydraVector) sql commands to alter table
  */
 public static Long sqlCmds;
 public static HydraVector getSqlCmds() {
   if (AlterTablePrms.tasktab().get(AlterTablePrms.sqlCmds) != null) {
     return AlterTablePrms.tasktab().vecAt(AlterTablePrms.sqlCmds);
   } else {
     return AlterTablePrms.tab().vecAt(AlterTablePrms.sqlCmds);
   }
 }

 /*
  * (HydraVector) sql commands to alter table.
  * Returns null (does not fail) if parameter is undefined.
  */
 public static Long sqlCmdsForPopulatedDB;
 public static HydraVector getSqlCmdsForPopulatedDB() {
   if (AlterTablePrms.tasktab().get(AlterTablePrms.sqlCmdsForPopulatedDB) != null) {
     return AlterTablePrms.tasktab().vecAt(AlterTablePrms.sqlCmdsForPopulatedDB);
   } else {
     if (AlterTablePrms.tab().get(AlterTablePrms.sqlCmdsForPopulatedDB) != null) {
       return AlterTablePrms.tab().vecAt(AlterTablePrms.sqlCmdsForPopulatedDB);
     } else {
       return null;
     }
   }
 }

 /*
  * (HydraVector) negative sql commands to alter table that should fail
  * Returns null (does not fail) if parameter is undefined.
  */
 public static Long sqlNegativeCmds;
 public static HydraVector getSqlNegativeCmds() {
   if (AlterTablePrms.tasktab().get(AlterTablePrms.sqlNegativeCmds) != null) {
     return AlterTablePrms.tasktab().vecAt(AlterTablePrms.sqlNegativeCmdsForPopulatedDB);
   } else {
     if (AlterTablePrms.tab().get(AlterTablePrms.sqlNegativeCmds) != null) {
       return AlterTablePrms.tab().vecAt(AlterTablePrms.sqlNegativeCmds);
     } else {
       return null;
     }
   }
  }

 /*
  * (HydraVector) negative sql commands to alter table that should fail
  * Returns null (does not fail) if parameter is undefined.
  */
 public static Long sqlNegativeCmdsForPopulatedDB;
 public static HydraVector getSqlNegativeCmdsForPopulatedDB() {
   if (AlterTablePrms.tasktab().get(AlterTablePrms.sqlNegativeCmdsForPopulatedDB) != null) {
     return AlterTablePrms.tasktab().vecAt(AlterTablePrms.sqlNegativeCmdsForPopulatedDB);
   } else {
     if (AlterTablePrms.tab().get(AlterTablePrms.sqlNegativeCmdsForPopulatedDB) != null) {
       return AlterTablePrms.tab().vecAt(AlterTablePrms.sqlNegativeCmdsForPopulatedDB);
     } else {
       return null;
     }
   }
  }
}
