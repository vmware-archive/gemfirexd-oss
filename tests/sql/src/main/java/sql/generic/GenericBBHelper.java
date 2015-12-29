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
package sql.generic;

import sql.SQLBB;
import sql.generic.ddl.ConstraintInfoHolder;
import sql.generic.ddl.SchemaInfo;
import sql.generic.ddl.TableInfo;

/**
 * GenericBBHelper
 * 
 * @author Namrata Thanvi
 */

public class GenericBBHelper {
 final static String tableSuffix = "_TableInfo" ;
 final static String schemaSuffix = "_SchemaInfo";
  
 public static TableInfo getTableInfo(String fullyQualifiedTableName){
   String fullTableName = fullyQualifiedTableName.toUpperCase();
   return (TableInfo) SQLBB.getBB().getSharedMap().get(fullTableName + tableSuffix);
 }
 
 public static void putTableInfo(TableInfo tableInfo){
   String fullTableName = tableInfo.getFullyQualifiedTableName().toUpperCase();
   SQLBB.getBB().getSharedMap().put(fullTableName + tableSuffix, tableInfo);
 }
 
 public static SchemaInfo getSchemaInfo(String schemaName){
   String schema = schemaName.toUpperCase();
   return (SchemaInfo) SQLBB.getBB().getSharedMap().get(schema + schemaSuffix);
 }
 
 public static void putSchemaInfo(SchemaInfo schema){
   String name = schema.getSchemaName().toUpperCase();
   SQLBB.getBB().getSharedMap().put(name + schemaSuffix, schema);
 }
 
 public static ConstraintInfoHolder getConstraintInfoHolder( ){
   return (ConstraintInfoHolder) SQLBB.getBB().getSharedMap().get("ConstraintInfoHolder");
 }
 
 public static void putConstraintInfoHolder(ConstraintInfoHolder constraintInfo){
   SQLBB.getBB().getSharedMap().put("ConstraintInfoHolder", constraintInfo);
 }
 
}
