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

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;

import com.pivotal.gemfirexd.app.tpce.TPCETxnInput;
import com.pivotal.gemfirexd.app.tpce.TPCETxnOutput;
import com.pivotal.gemfirexd.app.tpce.input.CustomerPositionTxnInput;
import com.pivotal.gemfirexd.app.tpce.jpa.entity.Customer;
import com.pivotal.gemfirexd.app.tpce.output.CustomerPositionTxnOutput;
/**
 * The Java class for the transaction Customer-Position.
 * TPC-E Section 3.3.2
 */

public class JpaCustomerPosition extends JpaTransaction {
	private static final String CUSTOMER_TID_QUERY = "FROM Customer c where c.cTaxId = :taxId";
	private static final String CUSTOMER_CID_QUERY = "FROM Customer c where c.cId = :cId";
    //This sql requires the LastTrade and HoldingSummary be colocated. Even relationship navigation does also not work
	//private static final String ASSET_CID_QUERY = "SELECT ca.caId, ca.caBal, hs.hsQty, lt.ltPrice FROM CustomerAccount ca JOIN ca.holdingSummary hs JOIN hs.security s JOIN s.lastTrade lt WHERE ca.customer.cId = :cId";
	private static final String ASSET_CID_QUERY = "SELECT ca.caId, ca.caBal, SUM(hs.hsQty*lt.ltPrice) FROM CustomerAccount ca LEFT OUTER JOIN ca.holdingSummary hs, LastTrade lt WHERE hs.id.hsSSymb = lt.ltSSymb AND ca.customer.cId = :cId GROUP BY ca.caId, ca.caBal ORDER BY 3 ASC";
	
	//this sql does not work in gemfirexd since it does not support the order by  in subquery
	//private static final String HISTORY_ACCTID_NATVIE_QUERY = "SELECT tr.T_ID, tr.T_S_SYMB, tr.T_QTY, st.ST_NAME, th.TH_DTS FROM (SELECT T_ID ID FROM TRADE WHERE T_CA_ID = :acct_id ORDER BY T_DTS DESC FETCH FIRST 10 ROWS ONLY) t, TRADE tr, TRADE_HISTORY th, STATUS_TYPE st WHERE tr.T_ID = t.ID AND th.TH_T_ID = tr.T_ID AND st.ST_ID = th.TH_ST_ID ORDER BY th.TH_DTS DESC FETCH FIRST 30 ROWS ONLY";
	//rewrite the sql into following with code enforcement to limit the T_ID to 10 -- bogus, see ticket 46854
	private static final String HISTORY_ACCTID_NATVIE_QUERY = "SELECT t.T_ID, t.T_S_SYMB, t.T_QTY, st.ST_NAME, th.TH_DTS FROM TRADE t, TRADE_HISTORY th, STATUS_TYPE st WHERE t.T_CA_ID = :tCaId AND th.TH_T_ID = t.T_ID AND st.ST_ID = th.TH_ST_ID ORDER BY th.TH_DTS DESC, t.T_DTS DESC";
	
	protected CustomerPositionTxnInput  cpTxnInput = null;
	protected CustomerPositionTxnOutput cpTxnOutput = null;
	
	public JpaCustomerPosition() {
	}
	
	@Override
	public TPCETxnOutput runTxn (TPCETxnInput txnInput) {
		cpTxnOutput = new CustomerPositionTxnOutput();
		cpTxnInput = (CustomerPositionTxnInput) txnInput;
		try {
			invokeFrame1();
			if (cpTxnInput.getHistory() == 1) {
				long historyAcctId = cpTxnOutput.getAcctIdArray().get(cpTxnInput.getAcctIdIndex());
				invokeFrame2(historyAcctId);
			} else {	
				invokeFrame3();
			}
		} finally {
			jpaTxnManager.close();
		}

    	return cpTxnOutput;
    }

	/* For testing the ticket 46865 (Error occurs in the left outer join of a table with a view)
	@SuppressWarnings("unchecked")
	protected void invokeFrame0(){
		beginTransaction();
		
		try {
			List<Object[]> hsArray = (List<Object[]>)entityManager.createQuery("SELECT ca.caId, ca.caBal, hs.hsQty FROM CustomerAccount ca LEFT OUTER JOIN ca.holdingSummary hs WHERE ca.caId = :acctId").setParameter("acctId", 43000007611L).getResultList();
			for (Object[] hs : hsArray) {
				Double qty = (Double)hs[0];			
			}
		} catch (RuntimeException re) {
			//Any JPA related exceptions are RuntimeException, catch, log and rethrow.
			throw re;
		}
	}
	*/
	
	@SuppressWarnings("unchecked")
	protected void invokeFrame1(){
		jpaTxnManager.beginTransaction();
		Customer customer = null;
		
		try {
			if (cpTxnInput.getCustId() == 0) {
				customer = (Customer)entityManager.createQuery(CUSTOMER_TID_QUERY).setParameter("taxId", cpTxnInput.getTaxId()).getSingleResult();
			} else {
				customer = (Customer)entityManager.createQuery(CUSTOMER_CID_QUERY).setParameter("cId", cpTxnInput.getCustId()).getSingleResult();
			}
		} catch (RuntimeException re) {
			jpaTxnManager.rollback();
			//Any JPA related exceptions are RuntimeException, catch, log and rethrow.
			throw re;
		}
		
		addCustomerToOutput(customer);
		
		List<Object[]> rs = null;
		try {
			rs = (List<Object[]>) entityManager.createQuery(ASSET_CID_QUERY).setParameter("cId", customer.getCId()).setMaxResults(CustomerPositionTxnOutput.MAX_ACCT_LEN).getResultList();
			//Generated SQL used in JPA is:
			//select customerac0_.CA_ID as col_0_0_, customerac0_.CA_BAL as col_1_0_, sum(holdingsum1_.HS_QTY*lasttrade2_.LT_PRICE) 
			//as col_2_0_ from CUSTOMER_ACCOUNT customerac0_ left outer join HOLDING_SUMMARY_TBL holdingsum1_ on 
			//customerac0_.CA_ID=holdingsum1_.HS_CA_ID, LAST_TRADE lasttrade2_ 
			//where holdingsum1_.HS_S_SYMB=lasttrade2_.LT_S_SYMB and customerac0_.CA_C_ID=? 
			//group by customerac0_.CA_ID , customerac0_.CA_BAL order by 3 ASC fetch first 10 rows only
		} catch (RuntimeException re) {
			jpaTxnManager.rollback();
			//Any JPA related exceptions are RuntimeException, catch, log and rethrow.
			throw re;
		}
		
		cpTxnOutput.setAcctLen(rs.size());
		if (cpTxnOutput.getAcctLen() < 1 || cpTxnOutput.getAcctLen() > CustomerPositionTxnOutput.MAX_ACCT_LEN) {
			//To-Do
			jpaTxnManager.rollback();
			int status = -121; //
			throw new IllegalStateException("Account length " + cpTxnOutput.getAcctLen() + " is not valid. Status: " + status);
		}
		addAssetsToOutput(rs);
	}
	
	@SuppressWarnings("unchecked")
	protected void invokeFrame2(long acctId){
		List<Object[]> rs = null;
		try {
			rs = (List<Object[]>) entityManager.createNativeQuery(HISTORY_ACCTID_NATVIE_QUERY).setParameter("tCaId", acctId).setMaxResults(CustomerPositionTxnOutput.MAX_HIST_LEN).getResultList();
			//Generated SQL used in JPA is:
		} catch (RuntimeException re) {
			jpaTxnManager.rollback();
			//Any JPA related exceptions are RuntimeException, catch, log and rethrow.
			throw re;
		}
		addHistoryToOutput(rs);	
		if (cpTxnOutput.getHistLen() < 10 || cpTxnOutput.getHistLen() > CustomerPositionTxnOutput.MAX_HIST_LEN) {
			//To-Do
			jpaTxnManager.rollback();
			int status = -121; //
			throw new IllegalStateException("History length for customer account " + acctId + " is " + cpTxnOutput.getHistLen() + " ;Not valid! Status: " + status);			
		}		
		jpaTxnManager.commit();
	}
	
	protected void invokeFrame3(){
		jpaTxnManager.commit();
	}
	
	private void addCustomerToOutput(Customer customer) {
		cpTxnOutput.setCId(customer.getCId());
		cpTxnOutput.setCAdId(customer.getCAdId());
		cpTxnOutput.setCArea1(customer.getCArea1());
		cpTxnOutput.setCArea2(customer.getCArea2());
		cpTxnOutput.setCArea3(customer.getCArea3());
		cpTxnOutput.setCCtry1(customer.getCCtry1());
		cpTxnOutput.setCCtry2(customer.getCCtry2());
		cpTxnOutput.setCCtry3(customer.getCCtry3());
		cpTxnOutput.setCDob(customer.getCDob());
		cpTxnOutput.setCEmail1(customer.getCEmail1());
		cpTxnOutput.setCEmail2(customer.getCEmail2());
		cpTxnOutput.setCExt1(customer.getCExt1());
		cpTxnOutput.setCExt2(customer.getCExt2());
		cpTxnOutput.setCExt3(customer.getCExt3());	
		cpTxnOutput.setCFName(customer.getCFName());
		cpTxnOutput.setCGndr(customer.getCGndr());
		cpTxnOutput.setCLName(customer.getCLName());
		cpTxnOutput.setCLocal1(customer.getCLocal1());
		cpTxnOutput.setCLocal2(customer.getCLocal2());
		cpTxnOutput.setCLocal3(customer.getCLocal3());
		cpTxnOutput.setCMName(customer.getCMName());
		cpTxnOutput.setCStId(customer.getCStId());
		cpTxnOutput.setCTier(customer.getCTier());		
	}
	
	private void addAssetsToOutput(List<Object[]> assets) {
		for (Object[] asset : assets){
			cpTxnOutput.getAcctIdArray().add((Long)asset[0]);
			cpTxnOutput.getCashBalArray().add((Double)asset[1]);
			cpTxnOutput.getAssetTotalArray().add(asset[2] == null ? 0: (Double)asset[2]);
		}		
	}
	
	private void addHistoryToOutput(List<Object[]> histories) {
		//The rs has the maximum 30 rows, but we should enforce different T_IDs should not exceed 10
		int distinctTradeIdSize = 0;
		for (Object[] history :histories){
			Long tradeId = ((BigInteger)history[0]).longValue();  //ticket 46871
			if (!cpTxnOutput.getTradeIdArray().contains(tradeId)) {
				distinctTradeIdSize++;
			}
			if (distinctTradeIdSize > 10) {
				break;
			}
			cpTxnOutput.getTradeIdArray().add(tradeId);  //why it returns the biginteger
			cpTxnOutput.getSymbolArray().add(((String)history[1]));
			cpTxnOutput.getQtyArray().add(((Integer)history[2]));
			cpTxnOutput.getTradeStatusArray().add(((String)history[3]));
			cpTxnOutput.getHistDtsArray().add(((Timestamp)history[4]));
		}	
		cpTxnOutput.setHistLen(cpTxnOutput.getTradeIdArray().size());
	}		


    //public static void main(String[] args ) {
    //	CustomerPositionTxnInput cpInput = new CustomerPositionTxnInput(0, /*4300000762L*/0, 1, "170HQ9038TX397"/*""*/); 
    //	JPACustomerPosition cp = new JPACustomerPosition();
    //	TPCETxnOutput cpOutput = cp.runTxn(cpInput);
    //}

}