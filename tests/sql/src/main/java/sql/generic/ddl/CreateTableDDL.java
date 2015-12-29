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
package sql.generic.ddl;

import java.util.List;

import sql.generic.GenericBBHelper;

public class CreateTableDDL {
  String tableName;
  TableInfo tableInfo;
  final String ddl;
  
  public CreateTableDDL(String fullyQualifiedtableName){
    this.tableName = fullyQualifiedtableName.toUpperCase();
    this.tableInfo = GenericBBHelper.getTableInfo(tableName);
    ddl = createDDL();
  }
  
  public CreateTableDDL(TableInfo tableInfo){
    this.tableInfo = tableInfo;
    this.tableName = tableInfo.getTableName();
    ddl = createDDL();
  }
  
  public String createDDL(){
    /*
    CREATE TABLE table-name {
         ( { column-definition | table-constraint } [ , { column-definition | table-constraint } ] * )
     | 
         [ ( column-name [, column-name ] * ) ]
         AS query-expression
         WITH NO DATA
     }

     [ partitioning-clause | REPLICATE ]
     [ server-groups-clause ]
     [ gateway-sender-clause ]
     [ async-event-listener-clause ]
     [ eviction-clause | eviction-by-criteria-clause]
     [ expiration-clause ] *
     [ persistence-clause ]
     [ hdfs-store-clause ]
     [ offheap-clause ]
*/    
    StringBuffer sb = new StringBuffer();
    sb.append(tableInfo.getDerbyDDL()).append(" ");
    
    //partition clause
    sb.append(tableInfo.getPartitioningClause()).append(" ");

    //server group
    if (tableInfo.getServerGroups().length() > 0
        && !tableInfo.getServerGroups().equalsIgnoreCase("default")) {
      String serverGroup = "SERVER GROUPS (" + tableInfo.getServerGroups()
          + ")";
      sb.append(serverGroup).append(" ");
    }

    //gateway sender
    List<String> senders = tableInfo.getGatewaySenderList();
    if(senders!=null && senders.size() > 0){
      String gateways = ""; 
      boolean first = true;
      for(String s: senders){
        gateways += (first) ? s : " , " + s;
      }
      sb.append("GATEWAYSENDER ( " +  gateways + " )").append(" ");
    }

    //async event listener
    List<String> asyncList = tableInfo.getAsyncEventListnerList();
    if(asyncList!=null && asyncList.size() > 0){
      String listners = ""; 
      boolean first = true;
      for(String s: asyncList){
        listners += (first) ? s : " , " + s;
      }
      sb.append("ASYNCEVENTLISTENER ( " +  listners + " )").append(" ");
    }
    
    //eviction clause
    String evictionClause = tableInfo.getEvictionClause();
    sb.append(evictionClause).append(" ");
    
    //todo - eviction by criteria
    //todo - expiration
    
    //persistent clause
    sb.append(tableInfo.getPersistClause()).append(" ");

    //hdfs clause
    String hdfsClause = tableInfo.getHdfsClause();
    sb.append(hdfsClause).append(" ");
    
    //offheap
    if (tableInfo.isOffHeap) {
      sb.append("OFFHEAP ");
    }

    return sb.toString();
  }

  public String getDDL() {
    return ddl;
  }
 
}
