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

import sql.joinStatements.*;
import util.TestException;

/**
 * @author eshu
 *
 */
public class JoinTableStmtsFactory {
  public static final int TRADE_CUSTOMERS_NETWORTH = 1;
  public static final int TRADE_SECURITIES_PORTFOLIO = 2;
  public static final int TRADE_CUSTOMERS_SECURITIES_PORTFOLIO = 3;  
  public static final int TRADE_CUSTOMERS_PORTFOLIO_SELLORDERS= 4;
  public static final int TRADE_MULTITABLES=5;
  
  public JoinTableStmtIF createQueryStmt(int whichJoin) {
    JoinTableStmtIF joinStmtIF;
    switch (whichJoin) {
    case TRADE_CUSTOMERS_NETWORTH:
      joinStmtIF = new CustomersNetworthJoinStmt();
      break;
    case TRADE_SECURITIES_PORTFOLIO:
      joinStmtIF = new SecuritiesPortfolioJoinStmt();
      break;
    case TRADE_CUSTOMERS_SECURITIES_PORTFOLIO:
      joinStmtIF = new CustomersSecuritiesPortfolioJoinStmt();
      break;
    case TRADE_CUSTOMERS_PORTFOLIO_SELLORDERS:
      joinStmtIF = new CustPortfSoJoinStmt();
      break;
    case TRADE_MULTITABLES:
      joinStmtIF = new MultiTablesJoinStmt();
      break;

    default:     
      throw new TestException("Unknown operation " + whichJoin);    
    }
    return joinStmtIF;
  }

  public static int getInt(String joinTable) {
    int whichJoin= -1;
    if (joinTable.equalsIgnoreCase("trade.customers_networth"))
      whichJoin = TRADE_CUSTOMERS_NETWORTH;
    else if (joinTable.equalsIgnoreCase("trade.securities_portfolio"))
      whichJoin = TRADE_SECURITIES_PORTFOLIO;
    else if (joinTable.equalsIgnoreCase("trade.customers_securities_portfolio"))
      whichJoin = TRADE_CUSTOMERS_SECURITIES_PORTFOLIO;
    else if (joinTable.equalsIgnoreCase("trade.customers_portfolio_sellorders"))
      whichJoin = TRADE_CUSTOMERS_PORTFOLIO_SELLORDERS;
    else if (joinTable.equalsIgnoreCase("trade.multitables"))
      whichJoin = TRADE_MULTITABLES;
    return whichJoin;
  }
  
}
