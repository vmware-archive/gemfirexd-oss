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
import java.util.List;

import com.pivotal.gemfirexd.app.tpce.TPCETxnHarnessStructs;
import com.pivotal.gemfirexd.app.tpce.TPCETxnInput;
import com.pivotal.gemfirexd.app.tpce.TPCETxnOutput;
import com.pivotal.gemfirexd.app.tpce.input.BrokerVolumeTxnInput;
import com.pivotal.gemfirexd.app.tpce.output.BrokerVolumeTxnOutput;
//import com.pivotal.gemfirexd.app.tpce.jpa.entity.*;
/**
 * The Java class for the transaction Broker-Volume.
 * TPC-E Section 3.3.1
 */

public class JpaBrokerVolume extends JpaTransaction {
	//This query need the table sector, industry, company, company_competitor replicated in server group Market_Group
	//Otherwise, split this query into two parts:
	//1. TRs associated with B_NAME
	//2. TRs associated with SC_NAME
	//Intersection using code
	private String brokerVolumeQueryTemplate = "select B_NAME, SUM(TR_QTY * TR_BID_PRICE) from TRADE_REQUEST, SECTOR, INDUSTRY, COMPANY, BROKER, SECURITY, CUSTOMER_ACCOUNT where TR_CA_ID = CA_ID and CA_B_ID = B_ID and TR_S_SYMB = S_SYMB and S_CO_ID = CO_ID and CO_IN_ID = IN_ID and SC_ID = IN_SC_ID and B_NAME in (%BROKER_LIST%) and SC_NAME = :scName group by B_NAME order by 2 DESC";
	
	protected BrokerVolumeTxnInput  bvTxnInput = null;
	protected BrokerVolumeTxnOutput bvTxnOutput = null;
	
	public JpaBrokerVolume() {
	}
	
	@Override
	public TPCETxnOutput runTxn (TPCETxnInput txnInput) {
		bvTxnOutput = new BrokerVolumeTxnOutput();
		bvTxnInput = (BrokerVolumeTxnInput)txnInput;
		try {
			invokeFrame1();
		} finally {
			jpaTxnManager.close();
		}
		
		if (bvTxnOutput.getListLen() < 0 || bvTxnOutput.getListLen() > TPCETxnHarnessStructs.max_broker_list_len) {
			int status = -111;
			bvTxnOutput.setStatus(status);
			//should throw exception?
			throw new IllegalStateException("Volume length " + bvTxnOutput.getListLen() + " is not valid. Status: " + status);
		}
    	return bvTxnOutput;
    }

	@SuppressWarnings("unchecked")
	protected void invokeFrame1(){	
		String query = buildBrokerVolumeQuery();
		jpaTxnManager.beginTransaction();
		List<Object[]> rs = null;
		try {
			rs = (List<Object[]>) entityManager.createNativeQuery(query).setParameter("scName", bvTxnInput.getSectorName()).getResultList();
		} catch (RuntimeException re) {
			jpaTxnManager.rollback();
			//Any JPA related exceptions are RuntimeException, catch, log and rethrow.
			throw re;
		}
		jpaTxnManager.commit();
		bvTxnOutput.setListLen(rs.size());
		addVolumeToOutput(rs);
	}
	
	private void addVolumeToOutput(List<Object[]> rs) {
		for (Object[] rsEntry : rs) {
			bvTxnOutput.getVolumeArray().add((Double)rsEntry[1]);
		}
	}
	
	private String buildBrokerVolumeQuery() {	
		if (bvTxnInput == null) {
			throw new IllegalStateException("Inproperly called buildBrokerVolumeQuery");
		}
		StringBuffer queryInClause = new StringBuffer();
		List<String> brokerList = bvTxnInput.getBrokerList();
		//the list should contains 20 - 40 elements according to the specification
		int brokerTotal = brokerList.size();
		int brokerCount = 0;
		for (String broker : brokerList) {
			brokerCount++;
			queryInClause.append("'").append(broker).append("'");
			if (brokerCount < brokerTotal) {
				queryInClause.append(", ");
			}
		}
		return brokerVolumeQueryTemplate.replaceAll("%BROKER_LIST%", queryInClause.toString());
	}
	
	/* Test Data: Sector - Technology/Broker: Sylvia P. Stieff, Patrick G. Coder
	 * select B_NAME, SUM(TR_QTY * TR_BID_PRICE) from TRADE_REQUEST, SECTOR, INDUSTRY, COMPANY, BROKER, SECURITY, CUSTOMER_ACCOUNT where TR_CA_ID = CA_ID and CA_B_ID = B_ID and TR_S_SYMB = S_SYMB and S_CO_ID = CO_ID and CO_IN_ID = IN_ID and SC_ID = IN_SC_ID and B_NAME in ('Sylvia P. Stieff', 'Patrick G. Coder') and SC_NAME = ? group by B_NAME order by 2 DESC
	 * 1. B_NAME -> B_ID -> B_CA_ID -> CA_ID -> TR_CA_ID ->TR_QTY * TR_BID_PRICE
	 * 2. SC_NAME -> SC_ID -> IN_SC_ID -> IN_ID -> CO_IN_ID ->CO_ID->S_CO_ID->S_SYMB->TR_S_SYMB
	 * Intersection of 1 & 2.
	 * insert into trade_request values (200000000256790, 'TMS', 'AUDC', 311, 21.57, 43000002587, 4300000005)
	 * insert into trade_request values (200000000343508, 'TMS', 'AWRE', 400, 20.57, 43000002521, 4300000005);
	 * insert into trade_request values (200000000331216, 'TLS', 'AWRE', 200, 25.54, 43000004615, 4300000007)
	 */
}