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
 package sql.sqlCallback.asyncEventListener;

import hydra.Log;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import sql.SQLHelper;

import com.pivotal.gemfirexd.callbacks.Event;

public class BasicAsyncListener extends AbstractAsyncListener {
  static {
    Log.getLogWriter().info("load BasicAsyncListener");
  }
  protected CustomersAsyncListener customers = new CustomersAsyncListener();
  protected NetworthAsyncListener networth = new NetworthAsyncListener();
  protected SecuritiesAsyncListener securities = new SecuritiesAsyncListener();
  protected PortfolioAsyncListener portfolio = new PortfolioAsyncListener();
  protected SellordersAsyncListener sellorders = new SellordersAsyncListener();
  protected BuyordersAsyncListener buyorders = new BuyordersAsyncListener();
  protected TxhistoryAsyncListener txhistory = new TxhistoryAsyncListener();
  protected EmployeesAsyncListener employees = new EmployeesAsyncListener();
  
  private AbstractAsyncListener getListener(String tableName) {
    if (tableName.equalsIgnoreCase("customers")) return customers;
    else if (tableName.equalsIgnoreCase("networth")) return networth;
    else if (tableName.equalsIgnoreCase("securities")) return securities;
    else if (tableName.equalsIgnoreCase("portfolio")) return portfolio;
    else if (tableName.equalsIgnoreCase("sellorders")) return sellorders;
    else if (tableName.equalsIgnoreCase("buyorders")) return buyorders;
    else if (tableName.equalsIgnoreCase("txhistory")) return txhistory;
    else if (tableName.equalsIgnoreCase("employees")) return employees;
    else {
      Log.getLogWriter().warning("testing configure issue, operation on unknown table");
      return null;
    }
  }  
    
  @Override
  public boolean processEvents(List<Event> events) {
    Log.getLogWriter().info("start processing the list of async events");
    boolean hasFailedEvent = false;
    for (Event event : events){
      /* each listener will set the failed event.
      if (failedEvent != null) {
        if (!isSame(failedEvent, event)) {
          if (!event.isPossibleDuplicate()) Log.getLogWriter().warning("an already processed " +
          		"event does not have isPossibleDuplicate() set to true");
          else Log.getLogWriter().info("this event has been processed before, do not retry");
          continue;
        } else {
          failedEvent = null; //reset the flag.
        }
      } 
      */
           
      String tableName = null;
      ResultSetMetaData meta = event.getResultSetMetaData();
      try {
        tableName = meta.getTableName(1);
      }catch(SQLException se){
        SQLHelper.handleSQLException(se);
      }

      AbstractAsyncListener listener = getListener(tableName);
      //List<Event> aList = new ArrayList<Event>();
      //aList.add(event);

      boolean success = listener.processEvent(event);
      if (!success & testUniqueKeys) {
        //only for testing unique keys case, otherwise test will hang as out of order delivery 
        //could cause the event continuously to fail due to sqlException
        
        hasFailedEvent = true;
        //do not return false right away, as inserts to parent and child table may be queued 
        //out of order even when each thread has its own set of keys
        //this will allow insert to parent to be successful if it is in the same batch
      }
      if (hasFailedEvent) return false;
    }
    return true;
  }
  
  protected void insert (Event event) throws SQLException {
    Log.getLogWriter().warning("shoud not be called here");  
  }
  
  protected void delete (Event event) throws SQLException {
    Log.getLogWriter().warning("shoud not be called here");
  }
  
  protected void update(Event event) throws SQLException{
    Log.getLogWriter().warning("shoud not be called here");
  }

  @Override
  public boolean processEvent(Event event) {
    Log.getLogWriter().warning("test issue, this method should not be called");
    return true;
  }

}
