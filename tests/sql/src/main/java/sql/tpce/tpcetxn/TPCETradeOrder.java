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
package sql.tpce.tpcetxn;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


import sql.SQLHelper;
import sql.tpce.TPCETest;
import sql.tpce.tpcedef.TPCEConstants;
import sql.tpce.tpcedef.TPCETxnInput;
import sql.tpce.tpcedef.TPCETxnOutput;
import sql.tpce.tpcedef.generator.CE;
import sql.tpce.tpcedef.generator.MEE;
import sql.tpce.tpcedef.input.TradeOrderTxnInput;
import sql.tpce.entity.Broker;
import sql.tpce.entity.Charge;
import sql.tpce.entity.ChargePK;
import sql.tpce.entity.Customer;
import sql.tpce.entity.CustomerAccount;
import sql.tpce.entity.Security;
import sql.tpce.entity.StatusType;
import sql.tpce.entity.Trade;
import sql.tpce.entity.TradeHistory;
import sql.tpce.entity.TradeHistoryPK;
import sql.tpce.entity.TradeInfo;
import sql.tpce.entity.TradeRequest;
import sql.tpce.entity.TradeType;
import sql.tpce.tpcedef.output.TradeOrderTxnOutput;
import hydra.Log;
import hydra.RemoteTestModule;
import util.TestException;
/**
 * The Java class for the transaction Trade-Order.
 * TPC-E Section 3.3.7
 */
//This class is implemented on purpose using various types of JPA queries 
//like NamedNativeQuery, NamedQuery, NativeQuery, Regular Query etc in order
//to see how they are supported with GemFireXD.
//It is of course not a good practice in the product code
public class TPCETradeOrder extends TPCETransaction {
	protected TradeOrderTxnInput  toTxnInput = null;
	protected TradeOrderTxnOutput toTxnOutput = null;
	protected Customer customer = null;
	protected CustomerAccount customerAccount = null;
	protected Broker broker = null;
	protected Connection conn = null;
	
	//Variables populated in frame 3 and used in frame 4
	BigDecimal commissionRate = null;
	BigDecimal chargeAmount = null;
	String statusId = null;
	String securitySymbol = null;
	BigDecimal requestedPrice = new BigDecimal("0.00");
	short ttIsMrkt = -1;
  long tradeId = 0;
	
	//Use regular query
	//private static final String CUSTOMER_ACCOUNT_QUERY = "from CustomerAccount ca where ca.caId = :caId";
	//private static final String PERMISSION_QUERY = "select ap.apAcl from AccountPermission ap where ap.id.apCaId = :caId and ap.id.apTaxId = :taxId and ap.apLName = :lName and ap.apFName = :fName";
	
	private static final String selectCustomerAccount = "select CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = ?";
	private static final String selectCustomer = 
	  " select C_F_NAME, C_L_NAME, C_TIER, C_TAX_ID from CUSTOMER where C_ID = ?" ;
	private static final String selectBroker = "select B_NAME from BROKER where B_ID = ?";
	
	private static final String selectAccountPermission = "select AP_ACL from ACCOUNT_PERMISSION where AP_CA_ID = ? and AP_F_NAME = ? and AP_L_NAME = ? and AP_TAX_ID = ?";
	
	//Define in Company as NamedQuery
	//private static final String COMPANY_NATIVE_QUERY1 = "select co_id from company where co_name = ?1";
	//private static final String COMPANY_NATIVE_QUERY2 = "select co_name from company where co_id = ?1";
	
	//Defined in Security entity as NamedNativeQuery
	//private static final String SECURITY_NATIVE_QUERY1 = "select s_ex_id, s_name, s_symb from security where s_co_id = ?1 and s_issue = ?2";
	//private static final String SECURITY_NATIVE_QUERY2 = "select s_co_id, s_ex_id, s_name from security where s_symb = ?1";
	private static final String selectCoId = "select CO_ID from COMPANY where CO_NAME = ?";
	private static final String selectSecurity = "select S_EX_ID, S_NAME, S_SYMB from SECURITY where S_CO_ID = ? and S_ISSUE = ?";
	
	private static final String selectCoIdWithSymb = "select s_co_id, s_ex_id, s_name from security where s_symb = ?";
	private static final String selectCoName = "select CO_NAME from COMPANY where CO_ID = ?";
	
	
	//private static final String LAST_TRADE_QUERY = "select lt_price from last_trade where lt_s_symb = ?1";
	private static final String selectLTPrice = "select lt_price from last_trade where lt_s_symb = ?";
	private static final String selectTType = "select tt_is_mrkt, tt_is_sell from trade_type where tt_id = ?";
	//Defined in the TradeType entity as NamedNativeQuery
	//private static final String TRADE_TYPE_QUERY = "select tt_is_mrkt, tt_is_sell from trade_type where tt_id = ?1";
	
	//private static final String HOLDING_SUMMARY_QUERY = "select hs_qty from holding_summary where hs_ca_id = ?1 and hs_s_symb =?2";
	private static final String selectHoldingSummary = "select hs_qty from holding_summary where hs_ca_id = ? and hs_s_symb =?";
	
	//Defined in Holding entity as the NamedQuery
	//private static final String HOLDING_DESC_QUERY = "select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS desc";
	//private static final String HOLDING_ASC_QUERY = "select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS asc";
	
	private static final String selectHoldingDesc = "select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS desc";
  private static final String selectHoldingAsc = "select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS asc";
	
  //JP Query on entities with ManyToMany relationship:
	//private static final String CUSTOMER_TOTAL_TAX_QUERY = "select sum(tr.txRate) from Taxrate tr, IN(tr.customers) c where c.cId = ?)";
	//select sum(taxrate0_.TX_RATE) as col_0_0_ from TAXRATE taxrate0_ inner join CUSTOMER_TAXRATE customers1_ on taxrate0_.TX_ID=customers1_.CX_TX_ID inner join CUSTOMER customer2_ on customers1_.CX_C_ID=customer2_.C_ID where customer2_.C_ID=?
	
	//To see if in/subquery as specified in TPCE works with GemFireXD, so have to use Native query
	//private static final String CUSTOMER_TOTAL_TAX_QUERY = "select sum(tx_rate) from taxrate where tx_id in (select cx_tx_id from customer_taxrate where cx_c_id = ?)";
  private static final String selectTaxRate = "select sum(tx_rate) from taxrate where tx_id in (select cx_tx_id from customer_taxrate where cx_c_id = ?)";
  
	//see named query in CommissionRate
	//private static final String COMMISSION_RATE_QUERY = "select cr_rate from commission_rate where cr_c_tier = ? and cr_tt_id = ? and cr_ex_id = ? and cr_from_qty <= ? and cr_to_qty >= ?";
  private static final String selectCommRate = "select cr_rate from commission_rate where cr_c_tier = ? and cr_tt_id = ? and cr_ex_id = ? and cr_from_qty <= ? and cr_to_qty >= ?";
	
  //Try the entityManager.Find
	//private static final String CHARGE_QUERY = "select ch_chrg from charge where ch_c_tier = ? and ch_tt_id = ?";
  private static final String selectCharge = "select ch_chrg from charge where ch_c_tier = ? and ch_tt_id = ?";
	
  private static final String selectAccountBal = "select CA_BAL from CUSTOMER_ACCOUNT where CA_ID = ?";
  
  //use native query since there is not direct relationship between HoldingSummary and LastTrade
	//private static final String HOLDING_ASSETS_QUERY = "select sum(hs_qty * lt_price) from holding_summary, last_trade where hs_ca_id = ? and lt_s_symb = hs_s_symb";
  private static final String selectHoldingAsset = "select sum(hs_qty * lt_price) from holding_summary, last_trade where hs_ca_id = ? and lt_s_symb = hs_s_symb";
  
  private static final String insertTrade = "insert into TRADE (T_ID, T_CA_ID, T_S_SYMB, T_ST_ID, T_BID_PRICE, T_CHRG, T_COMM, T_DTS, T_EXEC_NAME, T_IS_CASH, T_LIFO, T_QTY, T_TAX, T_TRADE_PRICE, T_TT_ID) " +
  		"values (default, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  private static final String insertTradeRequest = "insert into TRADE_REQUEST (TR_T_ID, TR_TT_ID, TR_S_SYMB, TR_QTY, TR_BID_PRICE, TR_CA_ID, TR_B_ID) values (?, ?, ?, ?, ?, ?, ?)";
	private static final String insertTradeHistory = "insert into TRADE_HISTORY(TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)";
  
	private static final String CONFLICTEXCEPTION = "X0Z02";
	
  public TPCETradeOrder() {
	}
	
	@Override
	public TPCETxnOutput runTxn(TPCETxnInput txnInput, Connection conn) throws SQLException {
		toTxnInput = (TradeOrderTxnInput)txnInput;
		toTxnOutput = new TradeOrderTxnOutput();
		this.conn = conn;
		MEE mee = new MEE();
		
		try {
		  try {
		    invokeFrame1();
		  } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
			if (!toTxnInput.getExecLastName().equalsIgnoreCase(customer.getCLName())
					|| !toTxnInput.getExecFirstName().equalsIgnoreCase(customer.getCFName())
					|| !toTxnInput.getExecTaxId().equalsIgnoreCase(customer.getCTaxId())) { 
			  try {
			    invokeFrame2();		
			  } catch (SQLException se) {
	        SQLHelper.handleSQLException(se);
	      }
			}	
			try {
			  invokeFrame3();	
			} catch (SQLException se) {
			  SQLHelper.handleSQLException(se);
			}
			try {
			  invokeFrame4();
			} catch (SQLException se) {
			  if (se.getSQLState().equals(CONFLICTEXCEPTION)) throw se; 
			  //due to #42672/#48685, will retry this operation 
			  else SQLHelper.handleSQLException(se);
      }
			try {
			  invokeFrame5();
			} catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
			
			try {
			  invokeFrame6(mee);
			} catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
		} catch (RuntimeException re) {
			//jpaTxnManager.rollback();
			throw re;
		} finally {
			//jpaTxnManager.close();
		}
 		return toTxnOutput;
	}

	protected void invokeFrame1() throws SQLException {
		//jpaTxnManager.beginTransaction();
	  long ca_b_id;
	  long ca_c_id;

		//SQL1: select CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = ?
		//Hibernate: select customerac0_.CA_ID as CA1_9_, customerac0_.CA_B_ID as CA5_9_, customerac0_.CA_BAL as CA2_9_, customerac0_.CA_NAME as CA3_9_, customerac0_.CA_TAX_ST as CA4_9_, customerac0_.CA_C_ID as CA6_9_ from CUSTOMER_ACCOUNT customerac0_ where customerac0_.CA_ID=? fetch first 2 rows only
		//It is not the bug from GemFireXD dialect when you see the fetch first 2 rows only for getSingleResult.
		//It is the hibernate implementation to avoid possible OOME if the setMaxResultSet is not called
		//and set the default resultset to 2 rows.
		
		PreparedStatement ps = conn.prepareStatement(selectCustomerAccount);
		ps.setLong(1, toTxnInput.getAcctId());
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
		  customerAccount = new CustomerAccount();
		  customerAccount.setCaName(rs.getString("CA_NAME"));
		  ca_b_id = rs.getInt("CA_B_ID");
		  ca_c_id = rs.getInt("CA_C_ID");
		  customerAccount.setCaTaxSt(rs.getShort("CA_TAX_ST"));
		  customerAccount.setCaId(toTxnInput.getAcctId());
		  
		  if (logDML) {
		    Log.getLogWriter().info(selectCustomerAccount + " gets CA_NAME: " + customerAccount.getCaName()
		        + " CA_B_ID:" + ca_b_id + " CA_C_ID: " + ca_c_id + " CA_TAX_ST: " + customerAccount.getCaTaxSt()
		        + " for CA_ID = " + toTxnInput.getAcctId());
		  }
		  if (rs.next()) {
		    toTxnOutput.setStatus(-711);
		    if (logDML) {
	        Log.getLogWriter().info(selectCustomerAccount + " gets CA_NAME: " + customerAccount.getCaName()
	            + " CA_B_ID:" + ca_b_id + " CA_C_ID: " + ca_c_id + " CA_TAX_ST: " + customerAccount.getCaTaxSt()
	            + " for CA_ID = " + toTxnInput.getAcctId());
	      }
		    throw new TestException ( selectCustomerAccount + " has more than 1 row " +
		    		"in result set for CA_ID = " + toTxnInput.getAcctId());
		  }
		} else {
		  toTxnOutput.setStatus(-711);
		  throw new TestException ( selectCustomerAccount + " does not get single row " +
          "in result set for CA_ID = " + toTxnInput.getAcctId());
		}
		
		rs.close();
		
	  //SQL2: select C_F_NAME, C_L_NAME, C_TIER, C_TAX_ID from CUSTOMER where C_ID = ?
		getBroker(ca_b_id);
		customerAccount.setBroker(broker);
		
	  //SQL3: select B_NAME from BROKER where B_ID = ?
		getCustomer(ca_c_id);
    customerAccount.setCustomer(customer);
    
		//SQL2: select C_F_NAME, C_L_NAME, C_TIER, C_TAX_ID from CUSTOMER where C_ID = ?
		//customer = (Customer)customerAccount.getCustomer();
		//SQL3: select B_NAME from BROKER where B_ID = ?
		//Hibernate: select broker0_.B_ID as B1_2_0_, broker0_.B_COMM_TOTAL as B2_2_0_, broker0_.B_NAME as B3_2_0_, broker0_.B_NUM_TRADES as B4_2_0_, broker0_.B_ST_ID as B5_2_0_ from BROKER broker0_ where broker0_.B_ID=?
		//broker = (Broker)customerAccount.getBroker();
	}
	
	protected void getBroker(long b_id) throws SQLException {
   PreparedStatement ps = conn.prepareStatement(selectBroker);
    ps.setLong(1, b_id);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      broker = new Broker();
      broker.setBName(rs.getString("B_NAME"));
      broker.setBId(b_id);
      if (logDML) {
        Log.getLogWriter().info(selectBroker + " gets B_NAME: " + broker.getBName()
             + " for B_ID = " + b_id);
      }
      if (rs.next()) {
        if (logDML) {
          Log.getLogWriter().info(selectBroker + " gets B_NAME: " + broker.getBName()
               + " for B_ID = " + b_id);
        }
        throw new TestException ( selectBroker + " has more than 1 row " +
            "in result set for B_ID = " + b_id);
      }
    } else {
      throw new TestException ( selectBroker + " does not get single row " +
          "in result set for B_ID = " + b_id);
    }
    rs.close();
	}
	
 protected void getCustomer(long c_id) throws SQLException {
   PreparedStatement ps = conn.prepareStatement(selectCustomer);
    ps.setLong(1, c_id);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      customer = new Customer();
      customer.setCFName(rs.getString("C_F_NAME"));
      customer.setCLName(rs.getString("C_L_NAME"));
      customer.setCTier(rs.getShort("C_TIER"));
      customer.setCTaxId(rs.getString("C_TAX_ID"));
      customer.setCId(c_id);
      
      if (logDML) {
        Log.getLogWriter().info(selectCustomer + " gets C_F_NAME: " + customer.getCFName()
            + " C_L_NAME:" + customer.getCLName() + " C_TIER: " + customer.getCTier() 
            + " C_TAX_ID: " + customer.getCTaxId()
            + " for C_ID = " + c_id);
      }
      if (rs.next()) {
        if (logDML) {
          Log.getLogWriter().info(selectCustomer + " gets C_F_NAME: " + customer.getCFName()
              + " C_L_NAME:" + customer.getCLName() + " C_TIER: " + customer.getCTier() 
              + " C_TAX_ID: " + customer.getCTaxId()
              + " for C_ID = " + c_id);
        }
        throw new TestException ( selectCustomer+ " has more than 1 row " +
            "in result set for C_ID = " + c_id);
      }
    } else {
      throw new TestException ( selectCustomer + " does not get single row " +
          "in result set for C_ID = " + c_id);
    }
    rs.close();
  }
	
	protected void invokeFrame2() throws SQLException{
		//SQL4: select AP_ACL from ACCOUNT_PERMISSION where AP_CA_ID = ? and AP_F_NAME = ? and AP_L_NAME = ? and AP_TAX_ID = ?
		//Regular JPA Query
		/*
	  List<String> apAclList = (List<String>) entityManager.createQuery(PERMISSION_QUERY)
				.setParameter("caId", toTxnInput.getAcctId())
				.setParameter("taxId", toTxnInput.getExecTaxId())
				.setParameter("lName", toTxnInput.getExecLastName())
				.setParameter("fName", toTxnInput.getExecFirstName())
				.getResultList();
		if (apAclList.get(0) == null) {
      toTxnOutput.setStatus(-721);
      throw new IllegalStateException("No account permission was found. Status: -721");
    }
		*/
	  
	  PreparedStatement ps = conn.prepareStatement(selectAccountPermission);
    ps.setLong(1, toTxnInput.getAcctId());
    ps.setString(2, toTxnInput.getExecFirstName());
    ps.setString(3, toTxnInput.getExecLastName());
    ps.setString(4, toTxnInput.getExecTaxId());    
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      if (logDML) {
        Log.getLogWriter().info(selectAccountPermission + "got account permission for " +
        		" AP_CA_ID = " + toTxnInput.getAcctId() + " AP_F_NAME = " + toTxnInput.getExecFirstName() +
        		" AP_L_NAME = " + toTxnInput.getExecLastName() + " AP_TAX_ID = " + toTxnInput.getExecTaxId());
      }
    } else {
      toTxnOutput.setStatus(-721);
      throw new TestException ( " No account permission was found: " + selectAccountPermission
          + " does not get single row " +
          "in result set for AP_CA_ID = " + toTxnInput.getAcctId()
          + " and AP_F_NAME = " + toTxnInput.getExecFirstName() 
          + " and AP_L_NAME = " + toTxnInput.getExecLastName()
          + " and AP_TAX_ID = " + toTxnInput.getExecTaxId());
    }
    
    rs.close();
	}
	
	@SuppressWarnings("unchecked")
	protected void invokeFrame3() throws SQLException{
		//s.sExId, s.sName, s.symb	
		long companyId = -1;
		String companyName = toTxnInput.getCoName();
		securitySymbol = toTxnInput.getSymbol().trim();
		String exchangeId = null;
		String securityName = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		BigDecimal marketPrice = null;
		
		if (securitySymbol.isEmpty()) {
			//SQL: select CO_ID from COMPANY where CO_NAME = ?
			//SQL: select S_EX_ID, S_NAME, S_SYMB from SECURITY where S_CO_ID = ? and S_ISSUE = ?
			//Native query
		  
		  ps = conn.prepareStatement(selectCoId);
		  ps.setString(1, companyName);
		  rs = ps.executeQuery();
		  
	    if (rs.next()) {
	      companyId = rs.getLong("CO_ID");
	      if (rs.next()) {
	        throw new TestException ( selectCoId + " has more than 1 row " +
	            "in result set for CO_NAME = " + companyName);
	      }
	    } else {
	      throw new TestException ( selectCoId + " does not get single row " +
	          "in result set for CO_NAME = " + companyName);
	    }	    
	    rs.close();
		  		  
			//companyId = ((BigInteger)entityManager.createNamedQuery("FindCompanyByName").setParameter(1, companyName).getSingleResult()).longValue();
			//Object[] securityArray = (Object[])entityManager.createNativeQuery(SECURITY_NATIVE_QUERY1).setParameter(1, companyId).setParameter(2, toTxnInput.getIssue()).getSingleResult();	
			//Named native query
			//Object[] securityArray = (Object[])entityManager.createNamedQuery("FindSecurityByCompany").setParameter(1, companyId).setParameter(2, toTxnInput.getIssue()).getSingleResult();	
			// exchangeId = (String)securityArray[0];
      // securityName = (String)securityArray[1];
      //securitySymbol = (String)securityArray[2];
	    
	    ps= conn.prepareStatement(selectSecurity);
      ps.setLong(1, companyId);
      ps.setString(2, toTxnInput.getIssue());
      rs = ps.executeQuery();
      
      if (rs.next()) {
        exchangeId = rs.getString("S_EX_ID");
        securityName = rs.getString("S_NAME");
        securitySymbol = rs.getString("S_SYMB");
        if (rs.next()) {
          throw new TestException ( selectSecurity + " has more than 1 row " +
              "in result set for CO_ID = " + companyId + " and S_ISSUE " + toTxnInput.getIssue());
        }
      } else {
        throw new TestException ( selectSecurity + " does not get single row " +
            "in result set for CO_ID = " + companyId + " and S_ISSUE " + toTxnInput.getIssue());
      }     
      rs.close();
		} else {
			//Object[] securityArray = (Object[])entityManager.createNativeQuery(SECURITY_NATIVE_QUERY2).setParameter(1, securitySymbol).getSingleResult();
			//Named native query
			//Object[] securityArray = (Object[])entityManager.createNamedQuery("FindSecurityBySymbol").setParameter(1, securitySymbol).getSingleResult();	
			//Native query
			//companyId = ((BigInteger)securityArray[0]).longValue();  //ticket 46871;
			//exchangeId = (String)securityArray[1];
			//securityName = (String)securityArray[2];
			//companyName = (String)entityManager.createNamedQuery("FindCompanyById").setParameter(1, companyId).getSingleResult();		
		  ps= conn.prepareStatement(selectCoIdWithSymb);
      ps.setString(1, securitySymbol);
      rs = ps.executeQuery();
      
      if (rs.next()) {
        companyId = rs.getLong("s_co_id");
        exchangeId = rs.getString("S_EX_ID");
        securityName = rs.getString("S_NAME");
        if (rs.next()) {
          throw new TestException ( selectCoIdWithSymb + " has more than 1 row " +
              "in result set for symb = " + securitySymbol);
        }
      } else {
        throw new TestException ( selectCoIdWithSymb + " does not get single row " +
            "in result set for symb = " + securitySymbol);
      }     
      rs.close();

      ps= conn.prepareStatement(selectCoName);
      ps.setLong(1, companyId);
      rs = ps.executeQuery();
      
      if (rs.next()) {
        companyName = rs.getString("CO_NAME");
        if (rs.next()) {
          throw new TestException ( selectCoName + " has more than 1 row " +
              "in result set for CO_ID = " + companyId);
        }
      } else {
        throw new TestException ( selectCoName + " does not get single row " +
            "in result set for CO_ID = " + companyId);
      }     
      rs.close();
		}
		
		//double marketPrice = (Double)entityManager.createNativeQuery(LAST_TRADE_QUERY).setParameter(1, securitySymbol).getSingleResult();
		//select tt_is_mrkt, tt_is_sell from trade_type where tt_id = ?1
		//gosh, this NamedNativeQuery is defined in the TradeType, without referring the sql in that class
		//could we have any idea about what are in the Object[]. How can people like such programming model?
		
		//get last trade price
    ps= conn.prepareStatement(selectLTPrice);
    ps.setString(1, securitySymbol);
    rs = ps.executeQuery();
    
    if (rs.next()) {
      marketPrice = rs.getBigDecimal("LT_PRICE");
      if (rs.next()) {
        throw new TestException ( selectLTPrice + " has more than 1 row " +
            "in result set for symb = " + securitySymbol);
      }
    } else {
      throw new TestException ( selectLTPrice + " does not get single row " +
          "in result set for symb = " + securitySymbol);
    }     
    rs.close();
		
		//Object[] tt = (Object[])entityManager.createNamedQuery("FindTradeOrderTTQuery").setParameter(1, toTxnInput.getTradeTypeId()).getSingleResult();
		
    //get order type
    ps= conn.prepareStatement(selectTType);
    ps.setString(1, toTxnInput.getTradeTypeId());
    rs = ps.executeQuery();
    short ttIsSell = -1;
    
    if (rs.next()) {
      ttIsMrkt = rs.getShort("tt_is_mrkt");
      ttIsSell = rs.getShort("tt_is_sell");
      if (rs.next()) {
        throw new TestException ( selectTType + " has more than 1 row " +
            "in result set for TT_ID = " + toTxnInput.getTradeTypeId());
      }
    } else {
      throw new TestException ( selectTType + " does not get single row " +
          "in result set for TT_ID = " + toTxnInput.getTradeTypeId());
    }     
    rs.close();
    
    requestedPrice = (ttIsMrkt == 1) ? marketPrice : toTxnInput.getRequestedPrice();
		
		int hsQty = 0;
		/*
		List<Integer> hsQtyList = (List<Integer>)entityManager.createNativeQuery(HOLDING_SUMMARY_QUERY)
				.setParameter(1, toTxnInput.getAcctId())
				.setParameter(2, securitySymbol)
				.getResultList();
	  */
		//select hs_qty from holding_summary where hs_ca_id = ? and hs_s_symb =?
    ps= conn.prepareStatement(selectHoldingSummary);
    ps.setLong(1, customerAccount.getCaId());
    ps.setString(2, securitySymbol);
    rs = ps.executeQuery();
    if (rs.next()) {
      hsQty = rs.getInt("hs_qty");
      if (rs.next()) {
        throw new TestException ( selectHoldingSummary + " has more than 1 row " +
            "in result set for hs_ca_id = " + customerAccount.getCaId() +
            " and hs_s_symb = " + securitySymbol);
      }
    } else {
      hsQty = 0;
      //No prior holdings exist - no rows returned
      if (logDML) {
        Log.getLogWriter().info(selectHoldingSummary + " does not get any row " +
            "for hs_ca_id = " + customerAccount.getCaId() +
            " and hs_s_symb = " + securitySymbol);
      }
          
    }           
    rs.close();
		
    BigDecimal buyValue = new BigDecimal("0.00") ;
    BigDecimal sellValue = new BigDecimal("0.00") ;
    long neededQty = toTxnInput.getTradeQty();
    
    int holdQty = 0;
    BigDecimal holdPrice = null;
    //private static final String selectHoldingDesc = "select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS desc";
    //private static final String selectHoldingAsc = "select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS asc";
    String select = null;
    if (toTxnInput.getIsLifo() == 1) {
      //FindHolidingByCaIdSymbDesc
      select = selectHoldingDesc;             
    } else {
      select = selectHoldingAsc; 
    } 
    ps= conn.prepareStatement(select);
    ps.setLong(1, customerAccount.getCaId());
    ps.setString(2, securitySymbol);
    rs = ps.executeQuery();
    
    //this is a sell transaction, so estimate the impact to any currently held long positions in the security
    if (ttIsSell == 1) {
    	if (hsQty > 0) {
    		/*
    		List<Object[]> holdingList = (List<Object[]>)holdingQuery
					.setParameter(1, toTxnInput.getAcctId())
					.setParameter(2, securitySymbol)
					.getResultList();
			
            for (int i = 0; i < holdingList.size() && neededQty != 0; i++) {
                Object[] holding = (Object[])holdingList.get(i);
                int holdQty = (Integer)holding[0];
                double holdPrice = (Double)holding[1];
                
                if (holdQty > neededQty) {
                    buyValue += neededQty * holdPrice;
                    sellValue += neededQty * requestedPrice;
                    neededQty = 0;
                }
                else {
                    buyValue += holdQty * holdPrice;
                    sellValue += holdQty * requestedPrice;
                    neededQty = neededQty - holdQty;
                }
            }
            //if the neededQty still > 0 , the customer would be liquidating all current holdings for 
            //this security and then creating a new short position for the remaining balance of this
            //transaction
             */

        while (rs.next() && neededQty == 0) {
          holdQty = rs.getInt("H_QTY");
          holdPrice = rs.getBigDecimal("H_PRICE");
          if (holdQty > neededQty) {
            buyValue = buyValue.add(holdPrice.multiply(new BigDecimal(String.valueOf(neededQty))));
            sellValue = sellValue.add(requestedPrice.multiply(new BigDecimal(String.valueOf(neededQty))));
            neededQty = 0;
          } else {
            buyValue = buyValue.add(holdPrice.multiply(new BigDecimal(String.valueOf(holdQty))));
            sellValue  = sellValue.add(requestedPrice.multiply(new BigDecimal(String.valueOf(holdQty))));
            neededQty = neededQty - holdQty;
          }
        }           
        rs.close();
    	}
    } else { //a buy transaction
    	if (hsQty < 0) {
    	  /*
    		Query holdingQuery = null;
    		if (toTxnInput.getIsLifo() == 1) {
    			//FindHolidingByCaIdSymbDesc
    			holdingQuery = entityManager.createNamedQuery("FindHolidingByCaIdSymbDesc");       			
    		} else {
    			holdingQuery = entityManager.createNamedQuery("FindHolidingByCaIdSymbAsc");
    		}
			List<Object[]> holdingList = (List<Object[]>)holdingQuery
					.setParameter(1, toTxnInput.getAcctId())
					.setParameter(2, securitySymbol)
					.getResultList();
			
            for (int i = 0; i < holdingList.size() && neededQty != 0; i++) {
                Object[] holding = (Object[])holdingList.get(i);
                int holdQty = (Integer)holding[0];
                double holdPrice = (Double)holding[1];
                if (holdQty + neededQty < 0) {
                    sellValue += neededQty * holdPrice;
                    buyValue += neededQty * requestedPrice;
                    neededQty = 0;
                }
                else {
                    holdQty = -holdQty;
                    sellValue += holdQty * holdPrice;
                    buyValue += holdQty * requestedPrice;
                    neededQty = neededQty - holdQty;
                }                    
            }
            //if the neededQty still > 0, the customer would cover all current short position for
            //this security  and then open a new long position for the remaining balance of this txn
    	}
      */ 
        while (rs.next() && neededQty == 0) {
          holdQty = rs.getInt("H_QTY");
          holdPrice = rs.getBigDecimal("H_PRICE");
          if (holdQty + neededQty < 0) {
            sellValue = sellValue.add(holdPrice.multiply(new BigDecimal(String.valueOf(neededQty))));
            buyValue = buyValue.add(requestedPrice.multiply(new BigDecimal(String.valueOf(neededQty))));
            neededQty = 0;
          } else {
            holdQty = -holdQty;
            sellValue = sellValue.add(holdPrice.multiply(new BigDecimal(String.valueOf(holdQty))));
            buyValue  = buyValue.add(requestedPrice.multiply(new BigDecimal(String.valueOf(holdQty))));
            neededQty = neededQty - holdQty;
          }
        }           
        rs.close();
    	} 
    }
    
    //estimate any capital gains tax that would be incurred as a result of this txn
    //actually, we can navigate from customer->taxrates, then aggregate, but it will not 
    //use the sql recommended by TPCE spec
    BigDecimal taxAmount = null;
    BigDecimal taxRates = null;
    if (sellValue.compareTo(buyValue) == 1 && (customerAccount.getCaTaxSt() == 1 || customerAccount.getCaTaxSt() == 2)) {
    	// BigDecimal taxRates = (BigDecimal)entityManager.createNativeQuery(CUSTOMER_TOTAL_TAX_QUERY).setParameter(1, customer.getCId()).getSingleResult();
    	//selectTaxRate = "select sum(tx_rate) from taxrate where tx_id in (select cx_tx_id from customer_taxrate where cx_c_id = ?)";
      ps= conn.prepareStatement(selectTaxRate);
      ps.setLong(1, customer.getCId());     
      rs = ps.executeQuery();
     
      if (rs.next()) {
        taxRates = rs.getBigDecimal(1);
        if (rs.next()) {
          throw new TestException ( selectTaxRate + " has more than 1 row " +
              "in result set for cx_c_id = " + customer.getCId());
        }
      } else {
        throw new TestException ( selectTaxRate + " does not get single row " +
            "in result set for cx_c_id = " + customer.getCId());
      }     
      rs.close();
      
      taxAmount = taxRates.multiply(sellValue.subtract(buyValue));

    }
    
    //selectCommRate = "select cr_rate from commission_rate where cr_c_tier = ? and cr_tt_id = ? and cr_ex_id = ? and cr_from_qty <= ? and cr_to_qty >= ?";
    ps= conn.prepareStatement(selectCommRate);
    ps.setShort(1, customer.getCTier()); 
    ps.setString(2, toTxnInput.getTradeTypeId());
    ps.setString(3, exchangeId);
    ps.setLong(4, toTxnInput.getTradeQty());
    ps.setLong(5, toTxnInput.getTradeQty());
    rs = ps.executeQuery();
   
    if (rs.next()) {
      commissionRate = rs.getBigDecimal("cr_rate");
      if (rs.next()) {
        throw new TestException ( selectCommRate + " has more than 1 row " +
            "in result set for cr_c_tier  = " + customer.getCTier() + 
            " and cr_tt_id = " + toTxnInput.getTradeTypeId() +
            " and cr_ex_id = " + exchangeId + 
            " and cr_from_qty & cr_to_qty = " + toTxnInput.getTradeQty());
      }
    } else {
      throw new TestException ( selectCommRate + " does not get single row " +
          "in result set for cr_c_tier  = " + customer.getCTier() + 
            " and cr_tt_id = " + toTxnInput.getTradeTypeId() +
            " and cr_ex_id = " + exchangeId + 
            " and cr_from_qty & cr_to_qty = " + toTxnInput.getTradeQty());
    }     
    rs.close();
    
    /*
        //select cr_rate from commission_rate where cr_c_tier = ? and cr_tt_id = ? and cr_ex_id = ? and cr_from_qty <= ? and cr_to_qty >= ?
        commissionRate = (BigDecimal)entityManager.createNamedQuery("FindCommissionRate")
        		.setParameter(1, customer.getCTier())
        		.setParameter(2,  toTxnInput.getTradeTypeId())
        		.setParameter(3, exchangeId)
        		.setParameter(4, (int)toTxnInput.getTradeQty()) 
        		//cast to int since the crFromQty is type of Integer in DB, otherwise, there will be an exception from NamedQuery/PositionParameter
        		.setParameter(5, (int)toTxnInput.getTradeQty())
        		.getSingleResult();
     */
    
        //SQL: select ch_chrg from charge where ch_c_tier = ? and ch_tt_id = ?
        //Hibernate: select charge0_.CH_C_TIER as CH1_4_0_, charge0_.CH_TT_ID as CH2_4_0_, charge0_.CH_CHRG as CH3_4_0_ from CHARGE charge0_ where charge0_.CH_C_TIER=? and charge0_.CH_TT_ID=?
        /*
        ChargePK chargePK = new ChargePK();
        chargePK.setChCTier(customer.getCTier());
        chargePK.setChTtId(toTxnInput.getTradeTypeId());
        Charge charge = (Charge)entityManager.find(Charge.class, chargePK);
        chargeAmount = charge.getChChrg();
        */
    
    //selectCharge = "select ch_chrg from charge where ch_c_tier = ? and ch_tt_id = ?";
    ps= conn.prepareStatement(selectCharge);
    ps.setShort(1, customer.getCTier()); 
    ps.setString(2, toTxnInput.getTradeTypeId());
    rs = ps.executeQuery();
   
    if (rs.next()) {
      chargeAmount  = new BigDecimal(String.valueOf(rs.getFloat("ch_chrg")));
      if (rs.next()) {
        throw new TestException ( selectCharge + " has more than 1 row " +
            "in result set for cr_c_tier  = " + customer.getCTier() + 
            " and cr_tt_id = " + toTxnInput.getTradeTypeId());
      }
    } else {
      throw new TestException ( selectCharge + " does not get single row " +
          "in result set for cr_c_tier  = " + customer.getCTier() + 
            " and cr_tt_id = " + toTxnInput.getTradeTypeId());
    }     
    rs.close();
        
    //compute assets on margin trades
    if (toTxnInput.getTypeIsMargin() == 1) {
      /*
        	double customerAssets = customerAccount.getCaBal();
        	//0 or 1 row
        	List<Double> holdAssetList = (List<Double>)entityManager.createNativeQuery(HOLDING_ASSETS_QUERY)
        			.setParameter(1, toTxnInput.getAcctId())
        			.getResultList();
        	if (holdAssetList.size() != 0) {
        		customerAssets = customerAssets + holdAssetList.get(0);
        	}
        	*/
     
      //selectAccountBal = "select CA_BAL from CUSTOMER_ACCOUNT where CA_ID = ?";
      BigDecimal customerAssets = null;
      ps= conn.prepareStatement(selectAccountBal);
      ps.setLong(1, toTxnInput.getAcctId()); 
      rs = ps.executeQuery();
     
      if (rs.next()) {
        customerAssets  = rs.getBigDecimal("CA_BAL");
        if (rs.next()) {
          throw new TestException ( selectAccountBal + " has more than 1 row " +
              "in result set for CA_ID  = " + toTxnInput.getAcctId());
        }
      } else {
        throw new TestException ( selectAccountBal + " does not get single row " +
            "in result set for CA_ID  = " + toTxnInput.getAcctId());
      }     
      rs.close();
      
      //selectHoldingAsset = "select sum(hs_qty * lt_price) from holding_summary, last_trade where hs_ca_id = ? and lt_s_symb = hs_s_symb";
      ps= conn.prepareStatement(selectHoldingAsset);
      ps.setLong(1, toTxnInput.getAcctId()); 
      rs = ps.executeQuery();
     
      if (rs.next()) {
        customerAssets  = customerAssets.add(rs.getBigDecimal(1));
        if (rs.next()) {
          throw new TestException ( selectHoldingAsset + " has more than 1 row " +
              "in result set for CA_ID  = " + toTxnInput.getAcctId());
        }
      }     
      rs.close();
    
    }
        
    //set the status for this trade
    statusId = (ttIsMrkt == 1) ? toTxnInput.getStSubmittedId() : toTxnInput.getStPendingId();	
        
    if (sellValue.compareTo(buyValue) == 1
    		&& (customerAccount.getCaTaxSt() == 1 || customerAccount.getCaTaxSt() == 2)
    		&& taxAmount.compareTo(new BigDecimal("0.00")) == 0) {
    	toTxnOutput.setStatus(-731);
    } else if (commissionRate.compareTo(new BigDecimal("0.0000")) == 0) {
    	toTxnOutput.setStatus(-732);
    } else if (chargeAmount.compareTo(new BigDecimal("0.00")) == 0) {
    	toTxnOutput.setStatus(-733);
    }
    if (toTxnInput.getRollItBack() == 0) { //to populate the results only when not to rollback
      toTxnOutput.setBuyValue(buyValue);
      toTxnOutput.setSellValue(sellValue);
      toTxnOutput.setTaxAmount(taxAmount);
    }
	}
	
	protected void invokeFrame4() throws SQLException{
	  /*
		//bump up the t_id to the max + 1 in the database
		//in order to improve the performance, change to always but there are issues (see 47442)
		//alter table trade alter t_id set generated always as identity;
		//e.g. alter table trade alter t_id restart with 200000000581761
		Trade trade = new Trade();
		//insert into TRADE (T_ID, T_CA_ID, T_S_SYMB, T_ST_ID, T_BID_PRICE, T_CHRG, T_COMM, T_DTS, T_EXEC_NAME, T_IS_CASH, T_LIFO, T_QTY, T_TAX, T_TRADE_PRICE, T_TT_ID) 
		//values (default, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		trade.setTDts(ts);
		//StatusType st = entityManager.getReference(StatusType.class, statusId);
		//trade.setStatusType(st);
		TradeType tt = entityManager.getReference(TradeType.class, toTxnInput.getTradeTypeId());
		trade.setTradeType(tt);
		trade.setTIsCash(toTxnInput.getTypeIsMargin() == 1 ? (short)0 : 1);
		Security security = entityManager.getReference(Security.class, securitySymbol);
		trade.setSecurity(security);
		trade.setTQty((int)toTxnInput.getTradeQty());
		trade.setTBidPrice(requestedPrice);
		trade.setCustomerAccount(customerAccount);
		trade.setTExecName(toTxnInput.getExecFirstName() + " " + toTxnInput.getExecLastName());
		//trade price
		trade.setTChrg(chargeAmount);
		trade.setTComm(commissionRate.doubleValue()/100*toTxnInput.getTradeQty()*requestedPrice);
		//
		trade.setTLifo((short)toTxnInput.getIsLifo());
		entityManager.persist(trade);
		entityManager.flush();
		long tradeId = trade.getTId();
		
		if (ttIsMrkt != 0) { //should be == 0, here just for testing purpose
			//insert into TRADE_REQUEST (TR_T_ID, TR_TT_ID, TR_S_SYMB, TR_QTY, TR_BID_PRICE, TR_CA_ID) values (?, ?, ?, ?, ?, ?)
			TradeRequest tr = new TradeRequest();
			tr.setTrTId(tradeId);
			tr.setTradeType(tt);
			tr.setSecurity(security);
			tr.setTrQty((int)toTxnInput.getTradeQty());
			tr.setTrBidPrice(requestedPrice);
			tr.setCustomerAccount(customerAccount);
			//have to set in TradeRequest Entity, but not required by TPCE
			tr.setBroker(broker);  //have to set in TradeRequest Entity
			entityManager.persist(tr);
		}
		
		//insert into TRADE_HISTORY(TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)
		TradeHistory th = new TradeHistory();
		TradeHistoryPK thPK = new TradeHistoryPK();
		thPK.setThStId(statusId);
		thPK.setThTId(tradeId);
		th.setId(thPK);
		th.setThDts(ts);
		entityManager.persist(th);
		if (toTxnInput.getRollItBack() == 0) {
			toTxnOutput.setTradeId(tradeId);
		}
		*/
	  
	  //insert into TRADE (T_ID, T_CA_ID, T_S_SYMB, T_ST_ID, T_BID_PRICE, T_CHRG, T_COMM, 
	  //T_DTS, T_EXEC_NAME, T_IS_CASH, T_LIFO, T_QTY, T_TAX, T_TRADE_PRICE, T_TT_ID) 
    //values (default, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
	  Timestamp orderTime = new Timestamp(System.currentTimeMillis());
    BigDecimal commission = commissionRate.multiply(requestedPrice).
      multiply(new BigDecimal(String.valueOf(toTxnInput.getTradeQty()))).
      divide(new BigDecimal("100"));
    String execName = toTxnInput.getExecFirstName() + " " + toTxnInput.getExecLastName();
    short isCash = toTxnInput.getTypeIsMargin() == 1 ? (short)0 : 1;
    
	  PreparedStatement ps= conn.prepareStatement(insertTrade, new String[]{"T_ID"});
	  ResultSet rs = null;
    ps.setLong(1, toTxnInput.getAcctId()); //T_CA_ID
    ps.setString(2, securitySymbol); // T_S_SYMB,
    ps.setString(3, statusId); // T_ST_ID
    ps.setBigDecimal(4, requestedPrice); // T_BID_PRICE
    ps.setBigDecimal(5, chargeAmount); //T_CHRG
    ps.setBigDecimal(6, commission); //T_COMM
    ps.setTimestamp(7, orderTime); //T_DTS
    ps.setString(8, execName);// T_EXEC_NAME
    ps.setShort(9, isCash); //T_IS_CASH
    ps.setShort(10, (short)toTxnInput.getIsLifo()); //T_LIFO
    ps.setInt(11, (int)toTxnInput.getTradeQty()); //T_QTY
    ps.setBigDecimal(12, new BigDecimal("0.00")); //T_TAX
    ps.setBigDecimal(13, null); //T_TRADE_PRICE
    ps.setString(14, toTxnInput.getTradeTypeId()); //T_TT_ID
     
    int count = ps.executeUpdate();
    rs = ps.getGeneratedKeys();
    if (rs.next())
      tradeId = rs.getLong(1);
    else
      throw new TestException("jdbc call getGeneratedKeys fail to returns the generated id " );
    
    if (tradeId == 0) {
      throw new TestException("jdbc call getGeneratedKeys returns wrong result " + tradeId);
    }
    
    if (logDML) {
      Log.getLogWriter().info(insertTrade + " with T_CA_ID = " + toTxnInput.getAcctId() + 
          " T_S_SYMB = " + securitySymbol + " T_ST_ID = " + statusId + " T_BID_PRICE " + requestedPrice +
          " T_CHRG = " + chargeAmount + " T_COMM = " + commission + " T_DTS = " + orderTime +
          " T_EXEC_NAME = " + execName + " T_IS_CASH = " + isCash + " T_LIFO = " + (short)toTxnInput.getIsLifo() +
          " T_QTY = " +  toTxnInput.getTradeQty() + " T_TAX = 0.00 T_TRADE_PRICE = null and " +
          " auto generated T_TT_ID = " + tradeId );
    }
    
    if (count != 1)
     throw new TestException("insert into trade table failed ");
       
    ps.close();
    
    //insert into trade request if not a market order
    if (ttIsMrkt != 1) { 
      //insert into TRADE_REQUEST (TR_T_ID, TR_TT_ID, TR_S_SYMB, TR_QTY, TR_BID_PRICE, TR_CA_ID, TR_B_ID) values (?, ?, ?, ?, ?, ?. ?)
      ps = conn.prepareStatement(insertTradeRequest);
      ps.setLong(1, tradeId); //TR_T_ID
      ps.setString(2, toTxnInput.getTradeTypeId()); //TR_TT_ID
      ps.setString(3, securitySymbol); //TR_S_SYMB
      ps.setInt(4, (int)toTxnInput.getTradeQty()); //TR_QTY
      ps.setBigDecimal(5, requestedPrice); //TR_BID_PRICE,
      ps.setLong(6, toTxnInput.getAcctId());//TR_CA_ID
      ps.setLong(7, broker.getBId());
      
      count = ps.executeUpdate();
      
      if (logDML) {
        Log.getLogWriter().info(insertTradeRequest + " with TR_T_ID = " + tradeId + 
            " TR_TT_ID = " + toTxnInput.getTradeTypeId() + " TR_S_SYMB = " + securitySymbol +
            " TR_QTY " + toTxnInput.getTradeQty() + " TR_BID_PRICE = " + requestedPrice +
            " TR_CA_ID = " + toTxnInput.getAcctId() + " TR_B_ID = " + broker.getBId());
      }
      
      if (count != 1)
       throw new TestException("insert into trade_request table failed ");  
      
      ps.close();
    }
    
    //insert into TRADE_HISTORY(TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)
    ps = conn.prepareStatement(insertTradeHistory);
    ps.setLong(1, tradeId); //TH_T_ID
    ps.setTimestamp(2, orderTime); //TH_DTS
    ps.setString(3, statusId); // TH_ST_ID
    
    count = ps.executeUpdate();
    
    if (logDML) {
      Log.getLogWriter().info(insertTradeHistory + " with TH_T_ID = " + tradeId + 
          " TH_DTS = " + orderTime + " TH_ST_ID = " + statusId);
    }
    
    if (count != 1)
      throw new TestException(insertTradeHistory + " should inserts 1 row " +
          "but inserted " + count + " row(s)");  
    
    ps.close();

    if (toTxnInput.getRollItBack() == 0) {
      toTxnOutput.setTradeId(tradeId);
      if (logDML) {
        Log.getLogWriter().info( tradeId + " will be committed.");
      }
    }
	  
	}
	

	protected void invokeFrame5() throws SQLException {
		if (toTxnInput.getRollItBack() == 1) {
			conn.rollback();
      if (logDML) {
        Log.getLogWriter().info( "This transaction is rolled back.");
      }
		}
	}
	
	protected void invokeFrame6(MEE mee) throws SQLException{
		if (toTxnInput.getRollItBack() != 1) {
			conn.commit();
      if (logDML) {
        Log.getLogWriter().info( "committed trade_order_txn");
      }
      
      if (RemoteTestModule.getCurrentThread().getCurrentTask().getTaskTypeString().
          equalsIgnoreCase("INITTASK")) {
        return; //do not send to market if the txn is run in inittask.
      }
      //int eAction = (ttIsMrkt == 1) ? TPCEConstants.eMEEProcessOrder : TPCEConstants.eMEESetLimitOrderTrigger;
      //SendToMarketFromHarness(requested_price, symbol, trade_id, trade_qty, trade_type_id, eAction)
      if (ttIsMrkt == 1) {
        TradeInfo ti = new TradeInfo();
        ti.setPriceQuotes(requestedPrice);
        ti.setSymbol(securitySymbol);
        ti.setTradeId(tradeId);
        ti.setTradeQty((int)toTxnInput.getTradeQty());
        ti.setTradeType(toTxnInput.getTradeTypeId());
        
        if (logDML) Log.getLogWriter().info("send trade info to market" + ti.toString());
        
        ArrayList<TradeInfo> tradeToMarket = new ArrayList<TradeInfo>();
        tradeToMarket.add(ti);
        mee.submitTradeToMarket(conn, tradeToMarket);
      }
		}
	}
}
