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
package com.pivotal.gemfirexd.app.tpce.jpa.txn;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;

import javax.persistence.NoResultException;
import javax.persistence.NonUniqueResultException;
import javax.persistence.Query;

import com.pivotal.gemfirexd.app.tpce.TPCEConstants;
import com.pivotal.gemfirexd.app.tpce.TPCETxnInput;
import com.pivotal.gemfirexd.app.tpce.TPCETxnOutput;
import com.pivotal.gemfirexd.app.tpce.input.TradeOrderTxnInput;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.Broker;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.Charge;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.ChargePK;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.Customer;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.CustomerAccount;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.Security;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.StatusType;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.Trade;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.TradeHistory;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.TradeHistoryPK;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.TradeRequest;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.TradeType;
import com.pivotal.gemfirexd.app.tpce.output.TradeOrderTxnOutput;
/**
 * The Java class for the transaction Trade-Order.
 * TPC-E Section 3.3.7
 */
//This class is implemented on purpose using various types of JPA queries 
//like NamedNativeQuery, NamedQuery, NativeQuery, Regular Query etc in order
//to see how they are supported with GemFireXD.
//It is of course not a good practice in the product code
public class JpaTradeOrder extends JpaTransaction {
	protected TradeOrderTxnInput  toTxnInput = null;
	protected TradeOrderTxnOutput toTxnOutput = null;
	protected Customer customer = null;
	protected CustomerAccount customerAccount = null;
	protected Broker broker = null;
	
	//Variables populated in frame 3 and used in frame 4
	BigDecimal commissionRate = null;
	double chargeAmount = 0;
	String statusId = null;
	String securitySymbol = null;
	double requestedPrice = 0;
	short ttIsMrkt = -1;
	
	//Use regular query
	private static final String CUSTOMER_ACCOUNT_QUERY = "from CustomerAccount ca where ca.caId = :caId";
	private static final String PERMISSION_QUERY = "select ap.apAcl from AccountPermission ap where ap.id.apCaId = :caId and ap.id.apTaxId = :taxId and ap.apLName = :lName and ap.apFName = :fName";
	
	//Define in Company as NamedQuery
	//private static final String COMPANY_NATIVE_QUERY1 = "select co_id from company where co_name = ?1";
	//private static final String COMPANY_NATIVE_QUERY2 = "select co_name from company where co_id = ?1";
	
	//Defined in Security entity as NamedNativeQuery
	//private static final String SECURITY_NATIVE_QUERY1 = "select s_ex_id, s_name, s_symb from security where s_co_id = ?1 and s_issue = ?2";
	//private static final String SECURITY_NATIVE_QUERY2 = "select s_co_id, s_ex_id, s_name from security where s_symb = ?1";
	
	private static final String LAST_TRADE_QUERY = "select lt_price from last_trade where lt_s_symb = ?1";
	
	//Defined in the TradeType entity as NamedNativeQuery
	//private static final String TRADE_TYPE_QUERY = "select tt_is_mrkt, tt_is_sell from trade_type where tt_id = ?1";
	
	private static final String HOLDING_SUMMARY_QUERY = "select hs_qty from holding_summary where hs_ca_id = ?1 and hs_s_symb =?2";
	
	//Defined in Holding entity as the NamedQuery
	//private static final String HOLDING_DESC_QUERY = "select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS desc";
	//private static final String HOLDING_ASC_QUERY = "select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS asc";
	
	//JP Query on entities with ManyToMany relationship:
	//private static final String CUSTOMER_TOTAL_TAX_QUERY = "select sum(tr.txRate) from Taxrate tr, IN(tr.customers) c where c.cId = ?)";
	//select sum(taxrate0_.TX_RATE) as col_0_0_ from TAXRATE taxrate0_ inner join CUSTOMER_TAXRATE customers1_ on taxrate0_.TX_ID=customers1_.CX_TX_ID inner join CUSTOMER customer2_ on customers1_.CX_C_ID=customer2_.C_ID where customer2_.C_ID=?
	
	//To see if in/subquery as specified in TPCE works with GemFireXD, so have to use Native query
	private static final String CUSTOMER_TOTAL_TAX_QUERY = "select sum(tx_rate) from taxrate where tx_id in (select cx_tx_id from customer_taxrate where cx_c_id = ?)";
	
	//see named query in CommissionRate
	//private static final String COMMISSION_RATE_QUERY = "select cr_rate from commission_rate where cr_c_tier = ? and cr_tt_id = ? and cr_ex_id = ? and cr_from_qty <= ? and cr_to_qty >= ?";
	
	//Try the entityManager.Find
	//private static final String CHARGE_QUERY = "select ch_chrg from charge where ch_c_tier = ? and ch_tt_id = ?";
	
	//use native query since there is not direct relationship between HoldingSummary and LastTrade
	private static final String HOLDING_ASSETS_QUERY = "select sum(hs_qty * lt_price) from holding_summary, last_trade where hs_ca_id = ? and lt_s_symb = hs_s_symb";
	
	public JpaTradeOrder() {
	}
	
	@Override
	public TPCETxnOutput runTxn(TPCETxnInput txnInput) {
		toTxnInput = (TradeOrderTxnInput)txnInput;
		toTxnOutput = new TradeOrderTxnOutput();
		try {
			invokeFrame1();
			if (!toTxnInput.getExecLastName().equalsIgnoreCase(customer.getCLName())
					|| !toTxnInput.getExecLastName().equalsIgnoreCase(customer.getCFName())
					|| !toTxnInput.getExecTaxId().equalsIgnoreCase(customer.getCTaxId())) { 
				invokeFrame2();		
			}	
			invokeFrame3();	
			invokeFrame4();
			invokeFrame5();
			invokeFrame6();
		} catch (RuntimeException re) {
			jpaTxnManager.rollback();
			throw re;
		} finally {
			jpaTxnManager.close();
		}
 		return toTxnOutput;
	}

	protected void invokeFrame1(){
		jpaTxnManager.beginTransaction();
		try {
			//SQL1: select CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = ?
			//Hibernate: select customerac0_.CA_ID as CA1_9_, customerac0_.CA_B_ID as CA5_9_, customerac0_.CA_BAL as CA2_9_, customerac0_.CA_NAME as CA3_9_, customerac0_.CA_TAX_ST as CA4_9_, customerac0_.CA_C_ID as CA6_9_ from CUSTOMER_ACCOUNT customerac0_ where customerac0_.CA_ID=? fetch first 2 rows only
			//It is not the bug from GemFireXD dialect when you see the fetch first 2 rows only for getSingleResult.
			//It is the hibernate implementation to avoid possible OOME if the setMaxResultSet is not called
			//and set the default resultset to 2 rows.
			customerAccount = (CustomerAccount)entityManager.createQuery(CUSTOMER_ACCOUNT_QUERY).setParameter("caId", toTxnInput.getAcctId()).getSingleResult();
		} catch (NoResultException nre) {
			toTxnOutput.setStatus(-711);
			throw nre;
		} catch (NonUniqueResultException nure) {
			toTxnOutput.setStatus(-711);
			throw nure;
		} catch(RuntimeException re) {
			//Any JPA related exceptions are RuntimeException, catch, log and rethrow.
			throw re;
		}
		//SQL2: select C_F_NAME, C_L_NAME, C_TIER, C_TAX_ID from CUSTOMER where C_ID = ?
		customer = (Customer)customerAccount.getCustomer();
		//SQL3: select B_NAME from BROKER where B_ID = ?
		//Hibernate: select broker0_.B_ID as B1_2_0_, broker0_.B_COMM_TOTAL as B2_2_0_, broker0_.B_NAME as B3_2_0_, broker0_.B_NUM_TRADES as B4_2_0_, broker0_.B_ST_ID as B5_2_0_ from BROKER broker0_ where broker0_.B_ID=?
		broker = (Broker)customerAccount.getBroker();
	}
	
	@SuppressWarnings("unchecked")
	protected void invokeFrame2(){
		//SQL4: select AP_ACL from ACCOUNT_PERMISSION where AP_CA_ID = ? and AP_F_NAME = ? and AP_L_NAME = ? and AP_TAX_ID = ?
		//Regular JPA Query
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
	}
	
	@SuppressWarnings("unchecked")
	protected void invokeFrame3(){
		//s.sExId, s.sName, s.symb	
		long companyId = -1;
		String companyName = toTxnInput.getCoName();
		securitySymbol = toTxnInput.getSymbol().trim();
		String exchangeId = null;
		String securityName = null;
		
		if (securitySymbol.isEmpty()) {
			//SQL: select CO_ID from COMPANY where CO_NAME = ?
			//SQL: select S_EX_ID, S_NAME, S_SYMB from SECURITY where S_CO_ID = ? and S_ISSUE = ?
			//Native query
			companyId = ((BigInteger)entityManager.createNamedQuery("FindCompanyByName").setParameter(1, companyName).getSingleResult()).longValue();
			//Object[] securityArray = (Object[])entityManager.createNativeQuery(SECURITY_NATIVE_QUERY1).setParameter(1, companyId).setParameter(2, toTxnInput.getIssue()).getSingleResult();	
			//Named native query
			Object[] securityArray = (Object[])entityManager.createNamedQuery("FindSecurityByCompany").setParameter(1, companyId).setParameter(2, toTxnInput.getIssue()).getSingleResult();	
			exchangeId = (String)securityArray[0];
			securityName = (String)securityArray[1];
			securitySymbol = (String)securityArray[2];
		} else {
			//Object[] securityArray = (Object[])entityManager.createNativeQuery(SECURITY_NATIVE_QUERY2).setParameter(1, securitySymbol).getSingleResult();
			//Named native query
			Object[] securityArray = (Object[])entityManager.createNamedQuery("FindSecurityBySymbol").setParameter(1, securitySymbol).getSingleResult();	
			//Native query
			companyId = ((BigInteger)securityArray[0]).longValue();  //ticket 46871;
			exchangeId = (String)securityArray[1];
			securityName = (String)securityArray[2];
			companyName = (String)entityManager.createNamedQuery("FindCompanyById").setParameter(1, companyId).getSingleResult();		
		}
		
		double marketPrice = (Double)entityManager.createNativeQuery(LAST_TRADE_QUERY).setParameter(1, securitySymbol).getSingleResult();
		//select tt_is_mrkt, tt_is_sell from trade_type where tt_id = ?1
		//gosh, this NamedNativeQuery is defined in the TradeType, without referring the sql in that class
		//could we have any idea about what are in the Object[]. How can people like such programming model?
		Object[] tt = (Object[])entityManager.createNamedQuery("FindTradeOrderTTQuery").setParameter(1, toTxnInput.getTradeTypeId()).getSingleResult();
		ttIsMrkt = (Short)tt[0];
		short ttIsSell = (Short)tt[1];
		requestedPrice = (ttIsMrkt == 1) ? marketPrice : toTxnInput.getRequestedPrice();
		
		int hsQty = 0;
		List<Integer> hsQtyList = (List<Integer>)entityManager.createNativeQuery(HOLDING_SUMMARY_QUERY)
				.setParameter(1, toTxnInput.getAcctId())
				.setParameter(2, securitySymbol)
				.getResultList();
		if (hsQtyList.size() > 0) {
			hsQty = hsQtyList.get(0);
		}

        double buyValue = 0;
        double sellValue = 0;
        long neededQty = toTxnInput.getTradeQty();
        
        //this is a sell transaction, so estimate the impact to any currently held long positions in the security
        if (ttIsSell == 1) {
        	if (hsQty > 0) {
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
        	}
        } else { //a buy transaction
        	if (hsQty < 0) {
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
        }
        
        //estimate any capital gains tax that would be incurred as a result of this txn
        //actually, we can navigate from customer->taxrates, then aggregate, but it will not 
        //use the sql recommended by TPCE spec
        double taxAmount = 0;
        if (sellValue > buyValue && (customerAccount.getCaTaxSt() == 1 || customerAccount.getCaTaxSt() == 2)) {
        	BigDecimal taxRates = (BigDecimal)entityManager.createNativeQuery(CUSTOMER_TOTAL_TAX_QUERY).setParameter(1, customer.getCId()).getSingleResult();
        	taxAmount = taxRates.doubleValue() * (sellValue - buyValue);
        }
        
        //select cr_rate from commission_rate where cr_c_tier = ? and cr_tt_id = ? and cr_ex_id = ? and cr_from_qty <= ? and cr_to_qty >= ?
        commissionRate = (BigDecimal)entityManager.createNamedQuery("FindCommissionRate")
        		.setParameter(1, customer.getCTier())
        		.setParameter(2,  toTxnInput.getTradeTypeId())
        		.setParameter(3, exchangeId)
        		.setParameter(4, (int)toTxnInput.getTradeQty()) 
        		//cast to int since the crFromQty is type of Integer in DB, otherwise, there will be an exception from NamedQuery/PositionParameter
        		.setParameter(5, (int)toTxnInput.getTradeQty())
        		.getSingleResult();
        //SQL: select ch_chrg from charge where ch_c_tier = ? and ch_tt_id = ?
        //Hibernate: select charge0_.CH_C_TIER as CH1_4_0_, charge0_.CH_TT_ID as CH2_4_0_, charge0_.CH_CHRG as CH3_4_0_ from CHARGE charge0_ where charge0_.CH_C_TIER=? and charge0_.CH_TT_ID=?
        ChargePK chargePK = new ChargePK();
        chargePK.setChCTier(customer.getCTier());
        chargePK.setChTtId(toTxnInput.getTradeTypeId());
        Charge charge = (Charge)entityManager.find(Charge.class, chargePK);
        chargeAmount = charge.getChChrg();
        
        //compute assets on margin trades
        if (toTxnInput.getTypeIsMargin() == 1) {
        	double customerAssets = customerAccount.getCaBal();
        	//0 or 1 row
        	List<Double> holdAssetList = (List<Double>)entityManager.createNativeQuery(HOLDING_ASSETS_QUERY)
        			.setParameter(1, toTxnInput.getAcctId())
        			.getResultList();
        	if (holdAssetList.size() != 0) {
        		customerAssets = customerAssets + holdAssetList.get(0);
        	}
        }
        
        //set the status for this trade
        statusId = (ttIsMrkt == 1) ? toTxnInput.getStSubmittedId() : toTxnInput.getStPendingId();	
        
        if ((sellValue > buyValue) 
        		&& (customerAccount.getCaTaxSt() == 1 || customerAccount.getCaTaxSt() == 2)
        		&& Math.abs(taxAmount - 0.00) < 0.005) {
        	toTxnOutput.setStatus(-731);
        } else if (Math.abs(commissionRate.doubleValue() - 0.0000) < 0.00005) {
        	toTxnOutput.setStatus(-732);
        } else if (Math.abs(chargeAmount - 0.00) < 0.005) {
        	toTxnOutput.setStatus(-733);
        }
        if (toTxnInput.getRollItBack() == 0) { //to populate the results only when not to rollback
	        toTxnOutput.setBuyValue(buyValue);
	        toTxnOutput.setSellValue(sellValue);
	        toTxnOutput.setTaxAmount(taxAmount);
        }
	}
	
	protected void invokeFrame4(){
		//bump up the t_id to the max + 1 in the database
		//in order to improve the performance, change to always but there are issues (see 47442)
		//alter table trade alter t_id set generated always as identity;
		//e.g. alter table trade alter t_id restart with 200000000581761
		Trade trade = new Trade();
		//insert into TRADE (T_ID, T_CA_ID, T_S_SYMB, T_ST_ID, T_BID_PRICE, T_CHRG, T_COMM, T_DTS, T_EXEC_NAME, T_IS_CASH, T_LIFO, T_QTY, T_TAX, T_TRADE_PRICE, T_TT_ID) 
		//values (default, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		Timestamp ts = new Timestamp(System.currentTimeMillis());
		trade.setTDts(ts);
		StatusType st = entityManager.getReference(StatusType.class, statusId);
		trade.setStatusType(st);
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
		//tax
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
	}
	

	protected void invokeFrame5(){
		if (toTxnInput.getRollItBack() == 1) {
			jpaTxnManager.rollback();
		}
	}
	
	protected void invokeFrame6(){
		if (toTxnInput.getRollItBack() != 1) {
			jpaTxnManager.commit();
	        //int eAction = (ttIsMrkt == 1) ? TPCEConstants.eMEEProcessOrder : TPCEConstants.eMEESetLimitOrderTrigger;
	        //SendToMarketFromHarness(requested_price, symbol, trade_id, trade_qty, trade_type_id, eAction)
		}
	}
}
