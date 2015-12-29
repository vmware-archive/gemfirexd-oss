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
package objects.query.broker;

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import objects.query.BaseQueryFactory;

public class SQLBrokerTicketQueryFactory extends BaseQueryFactory {

  //----------------------------------------------------------------------------
  // tables
  //----------------------------------------------------------------------------

  public List getTableStatements() {
    List stmts = new ArrayList();
    stmts
        .add("create table "
            + BrokerTicket.getTableName()
            + " (id int, brokerId int, price double, quantity int, ticker varchar(20), "
            + "str01 varchar(20), str02 varchar(20), str03 varchar(20), str04 varchar(20), str05 varchar(20), "
            + "str06 varchar(20), str07 varchar(20), str08 varchar(20), str09 varchar(20), str10 varchar(20), "
            + "str11 varchar(20), str12 varchar(20), str13 varchar(20), str14 varchar(20), str15 varchar(20))");
    return stmts;
  }
  
  public List getDropTableStatements() {
    List stmts = new ArrayList();
    stmts.add("drop table if exists " + BrokerTicket.getTableName());
    return stmts;
  }

  //----------------------------------------------------------------------------
  // inserts
  //----------------------------------------------------------------------------

  /**
   * Generate the list of insert statements required to create {@link
   * BrokerPrms#numTicketsPerBroker} tickets for the broker with the given
   * broker id.
   *
   * @param bid the unique broker id
   */
  public List getInsertStatements(int bid) {
    int numTicketsPerBroker = BrokerPrms.getNumTicketsPerBroker();
    int numTicketPrices = BrokerPrms.getNumTicketPrices();
    List stmts = new ArrayList();
    for (int i = 0; i < numTicketsPerBroker; i++) {
      int id = BrokerTicket.getId(i, bid, numTicketsPerBroker);
      double price = BrokerTicket.getPrice(id, numTicketPrices);
      int quantity = BrokerTicket.getQuantity(id);
      String ticker = BrokerTicket.getTicker(id);
      String str01 = BrokerTicket.getFiller(id);
      String str02 = BrokerTicket.getFiller(id);
      String str03 = BrokerTicket.getFiller(id);
      String str04 = BrokerTicket.getFiller(id);
      String str05 = BrokerTicket.getFiller(id);
      String str06 = BrokerTicket.getFiller(id);
      String str07 = BrokerTicket.getFiller(id);
      String str08 = BrokerTicket.getFiller(id);
      String str09 = BrokerTicket.getFiller(id);
      String str10 = BrokerTicket.getFiller(id);
      String str11 = BrokerTicket.getFiller(id);
      String str12 = BrokerTicket.getFiller(id);
      String str13 = BrokerTicket.getFiller(id);
      String str14 = BrokerTicket.getFiller(id);
      String str15 = BrokerTicket.getFiller(id);

      String stmt = "insert into " +
          BrokerTicket.getTableName() +
          " (id, brokerId, price, quantity, ticker, str01, str02, str03, str04, str05"
          + ", str06, str07, str08, str09, str10, str11, str12, str13, str14, str15)"
          + " values (" + id + "," + bid + "," + price + "," + quantity + ",'" + ticker
          + "','" + str01 + "','" + str02 + "','" + str03 + "','" + str04 + "','" + str05
          + "','" + str06 + "','" + str07 + "','" + str08 + "','" + str09 + "','" + str10
          + "','" + str11 + "','" + str12 + "','" + str13 + "','" + str14 + "','" + str15
          + "')";
      stmts.add(stmt);
    }
    return stmts;
  }

  public List getPreparedInsertStatements() {
    List stmts = new ArrayList();
    String stmt = "insert into " + BrokerTicket.getTableName() + "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    stmts.add(stmt);
    return stmts;
  }

  public int fillAndExecutePreparedInsertStatements(List pstmts, List stmts, int bid)
      throws SQLException {
    int results = 0;
    int numTicketsPerBroker = BrokerPrms.getNumTicketsPerBroker();
    int numTicketPrices = BrokerPrms.getNumTicketPrices();
    PreparedStatement stmt = (PreparedStatement) pstmts.get(0);
    for (int i = 0; i < numTicketsPerBroker; i++) {
      int id = BrokerTicket.getId(i, bid, numTicketsPerBroker);
      stmt.setInt(1, id);
      stmt.setInt(2, bid);
      stmt.setDouble(3, BrokerTicket.getPrice(id, numTicketPrices));
      stmt.setInt(4, BrokerTicket.getQuantity(id));
      stmt.setString(5, BrokerTicket.getTicker(id));
      stmt.setString(6, BrokerTicket.getFiller(id)); // str01
      stmt.setString(7, BrokerTicket.getFiller(id)); // str02
      stmt.setString(8, BrokerTicket.getFiller(id)); // str03
      stmt.setString(9, BrokerTicket.getFiller(id)); // str04
      stmt.setString(10, BrokerTicket.getFiller(id)); // str05
      stmt.setString(11, BrokerTicket.getFiller(id)); // str06
      stmt.setString(12, BrokerTicket.getFiller(id)); // str07
      stmt.setString(13, BrokerTicket.getFiller(id)); // str08
      stmt.setString(14, BrokerTicket.getFiller(id)); // str09
      stmt.setString(15, BrokerTicket.getFiller(id)); // str10
      stmt.setString(16, BrokerTicket.getFiller(id)); // str11
      stmt.setString(17, BrokerTicket.getFiller(id)); // str12
      stmt.setString(18, BrokerTicket.getFiller(id)); // str13
      stmt.setString(19, BrokerTicket.getFiller(id)); // str14
      stmt.setString(20, BrokerTicket.getFiller(id)); // str15
      if (logUpdates) {
        Log.getLogWriter().info("Executing update: " + stmt);
      }
      results += stmt.executeUpdate();
      if (logUpdates) {
        Log.getLogWriter().info("Executed update: " + stmt);
      }
    }
    return results;
  }

  //----------------------------------------------------------------------------
  // execution
  //----------------------------------------------------------------------------

  public void execute(String stmt, Connection conn) throws SQLException {
    String s = "Please execute on objects.query.broker.Broker instead";
    throw new UnsupportedOperationException(s);
  }
}
