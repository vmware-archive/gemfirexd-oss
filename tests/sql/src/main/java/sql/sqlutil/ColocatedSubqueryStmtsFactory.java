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
package sql.sqlutil;

import sql.subqueryStatements.*;
import sql.subqueryStatements.SubqueryStmtIF;
import util.TestException;

public class ColocatedSubqueryStmtsFactory {
  public static final int TRADE_CUSTOMERS_NETWORTH = 1;
  public static final int TRADE_SECURITIES_PORTFOLIO = 2;
  public static final int TRADE_CUSTOMERS_SECURITIES_PORTFOLIO = 3;  
  public static final int TRADE_CUSTOMERS_PORTFOLIO_SELLORDERS= 4;
  public static final int TRADE_MULTITABLES=5;
  
  public SubqueryStmtIF createQueryStmt(int whichSubquery) {
    SubqueryStmtIF subqueryStmtIF;
    switch (whichSubquery) {
    case TRADE_CUSTOMERS_NETWORTH:
      subqueryStmtIF = new CustomersNetworthSubqueryStmt();
      break;
    case TRADE_SECURITIES_PORTFOLIO:
      subqueryStmtIF = new SecuritiesPortfolioSubqueryStmt();
      break;
    case TRADE_CUSTOMERS_SECURITIES_PORTFOLIO:
      subqueryStmtIF = new CustomersSecuritiesPortfolioSubqueryStmt();
      break;
    case TRADE_CUSTOMERS_PORTFOLIO_SELLORDERS:
      subqueryStmtIF = new CustPortfSoSubqueryStmt();
      break;
    case TRADE_MULTITABLES:
      subqueryStmtIF = new MultiTablesSubqueryStmt();
      break;
    default:     
      throw new TestException("Unknown operation " + whichSubquery);    

    }
    return subqueryStmtIF;
  }

  public static int getInt(String subquery) {
    int whichSubquery= -1;
    if (subquery.equalsIgnoreCase("trade.customers_networth"))
      whichSubquery = TRADE_CUSTOMERS_NETWORTH;
    else if (subquery.equalsIgnoreCase("trade.securities_portfolio"))
      whichSubquery = TRADE_SECURITIES_PORTFOLIO;
    else if (subquery.equalsIgnoreCase("trade.customers_securities_portfolio"))
      whichSubquery = TRADE_CUSTOMERS_SECURITIES_PORTFOLIO;
    else if (subquery.equalsIgnoreCase("trade.customers_portfolio_sellorders"))
      whichSubquery = TRADE_CUSTOMERS_PORTFOLIO_SELLORDERS;
    else if (subquery.equalsIgnoreCase("trade.multitables"))
      whichSubquery = TRADE_MULTITABLES;
    return whichSubquery;
  }
}
