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
package sql.tpce.tpcedef.output;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import sql.tpce.tpcedef.TPCETxnOutput;
/**
 * The transaction Customer-Position output
 * TPC-E Section 3.3.2
 */

public class CustomerPositionTxnOutput implements TPCETxnOutput {
	private static final long serialVersionUID = 1L;
	
	public static final int MAX_ACCT_LEN = 10;
	public static final int MAX_HIST_LEN = 30;

	//customer information
	private long cId;
	private long cAdId;
	private String cArea1;
	private String cArea2;
	private String cArea3;
	private String cCtry1;
	private String cCtry2;
	private String cCtry3;
	private Date cDob;
	private String cEmail1;
	private String cEmail2;
	private String cExt1;
	private String cExt2;
	private String cExt3;
	private String cFName;
	private String cGndr;
	private String cLName;
	private String cLocal1;
	private String cLocal2;
	private String cLocal3;
	private String cMName;
	private String cStId;
	private short cTier;
	
	//We can use map or value object to store these data if we do follow the TPCE output format
	private int acctLen; //number of customer accounts <= MAX_ACCT_LEN
	private List<Long> acctIdArray = new ArrayList<Long>();
	private List<Double> assetTotalArray = new ArrayList<Double>();
	private List<Double> cashBalArray = new ArrayList<Double>(); ;
	
	private int histLen;  // <= MAX_HIST_LEN
	private List<Timestamp> histDtsArray = new ArrayList<Timestamp>();
	private List<Integer> qtyArray = new ArrayList<Integer>();
	private List<String> symbolArray = new ArrayList<String>();
	private List<Long> tradeIdArray = new ArrayList<Long>();
	private List<String> tradeStatusArray = new ArrayList<String>();

	public long getCId() {
		return this.cId;
	}

	public void setCId(long cId) {
		this.cId = cId;
	}

	public long getCAdId() {
		return this.cAdId;
	}

	public void setCAdId(long cAdId) {
		this.cAdId = cAdId;
	}
	
	public String getCArea1() {
		return this.cArea1;
	}

	public void setCArea1(String cArea1) {
		this.cArea1 = cArea1;
	}

	public String getCArea2() {
		return this.cArea2;
	}

	public void setCArea2(String cArea2) {
		this.cArea2 = cArea2;
	}

	public String getCArea3() {
		return this.cArea3;
	}

	public void setCArea3(String cArea3) {
		this.cArea3 = cArea3;
	}

	public String getCCtry1() {
		return this.cCtry1;
	}

	public void setCCtry1(String cCtry1) {
		this.cCtry1 = cCtry1;
	}

	public String getCCtry2() {
		return this.cCtry2;
	}

	public void setCCtry2(String cCtry2) {
		this.cCtry2 = cCtry2;
	}

	public String getCCtry3() {
		return this.cCtry3;
	}

	public void setCCtry3(String cCtry3) {
		this.cCtry3 = cCtry3;
	}

	public Date getCDob() {
		return this.cDob;
	}

	public void setCDob(Date cDob) {
		this.cDob = cDob;
	}

	public String getCEmail1() {
		return this.cEmail1;
	}

	public void setCEmail1(String cEmail1) {
		this.cEmail1 = cEmail1;
	}

	public String getCEmail2() {
		return this.cEmail2;
	}

	public void setCEmail2(String cEmail2) {
		this.cEmail2 = cEmail2;
	}

	public String getCExt1() {
		return this.cExt1;
	}

	public void setCExt1(String cExt1) {
		this.cExt1 = cExt1;
	}

	public String getCExt2() {
		return this.cExt2;
	}

	public void setCExt2(String cExt2) {
		this.cExt2 = cExt2;
	}

	public String getCExt3() {
		return this.cExt3;
	}

	public void setCExt3(String cExt3) {
		this.cExt3 = cExt3;
	}

	public String getCFName() {
		return this.cFName;
	}

	public void setCFName(String cFName) {
		this.cFName = cFName;
	}

	public String getCGndr() {
		return this.cGndr;
	}

	public void setCGndr(String cGndr) {
		this.cGndr = cGndr;
	}

	public String getCLName() {
		return this.cLName;
	}

	public void setCLName(String cLName) {
		this.cLName = cLName;
	}

	public String getCLocal1() {
		return this.cLocal1;
	}

	public void setCLocal1(String cLocal1) {
		this.cLocal1 = cLocal1;
	}

	public String getCLocal2() {
		return this.cLocal2;
	}

	public void setCLocal2(String cLocal2) {
		this.cLocal2 = cLocal2;
	}

	public String getCLocal3() {
		return this.cLocal3;
	}

	public void setCLocal3(String cLocal3) {
		this.cLocal3 = cLocal3;
	}

	public String getCMName() {
		return this.cMName;
	}

	public void setCMName(String cMName) {
		this.cMName = cMName;
	}

	public String getCStId() {
		return this.cStId;
	}

	public void setCStId(String cStId) {
		this.cStId = cStId;
	}
	
	public void setCTaxId(String cStId) {
		this.cStId = cStId;
	}

	public short getCTier() {
		return this.cTier;
	}

	public void setCTier(short cTier) {
		this.cTier = cTier;
	}

	public int getAcctLen() {
		return this.acctLen;
	}
	
	public void setAcctLen(int acctLen) {
		this.acctLen = acctLen;
	}
	
	public List<Long> getAcctIdArray() {
		return this.acctIdArray;
	}
	
	public void setAcctIdArray(List<Long> acctIdArray) {
		this.acctIdArray = acctIdArray;
	}

	public List<Double> getAssetTotalArray() {
		return this.assetTotalArray;
	}
	
	public void setAssetTotalArray(List<Double> assetTotalArray) {
		this.assetTotalArray = assetTotalArray;
	}
	
	public List<Double> getCashBalArray() {
		return this.cashBalArray;
	}
	
	public void setCashBalArray(List<Double> cashBalArray) {
		this.cashBalArray = cashBalArray;
	}
	
	public int getHistLen() {
		return this.histLen;
	}
	
	public void setHistLen(int histLen) {
		this.histLen = histLen;
	}
	
	public List<Timestamp> getHistDtsArray() {
		return this.histDtsArray;
	}
	
	public void setHistDtsArray(List<Timestamp> histDtsArray) {
		this.histDtsArray = histDtsArray;
	}

	public List<Integer> getQtyArray() {
		return this.qtyArray;
	}
	
	public void setQtyArray(List<Integer> qtyArray) {
		this.qtyArray = qtyArray;
	}
	
	public List<String> getSymbolArray() {
		return this.symbolArray;
	}
	
	public void setSymbolArray(List<String> symbolArray) {
		this.symbolArray = symbolArray;
	}	
	
	public List<Long> getTradeIdArray() {
		return this.tradeIdArray;
	}
	
	public void setTradeIdArray(List<Long> tradeIdArray) {
		this.tradeIdArray = tradeIdArray;
	}
	
	public List<String> getTradeStatusArray() {
		return this.tradeStatusArray;
	}
	
	public void setTradeStatusArray(List<String> tradeStatusArray) {
		this.tradeStatusArray = tradeStatusArray;
	}
	
	public String toString() {
		StringBuffer printOut = new StringBuffer("Customer Id: " + cId).append("\n\nAccounts:");
		for (int i = 0; i < acctLen; i++) {
			printOut.append("\nAccount: ").append(acctIdArray.get(i))
					.append(";Cash Balance: ").append(cashBalArray.get(i))
					.append(";Asset Total: ").append(assetTotalArray.get(i));
		}
		
		printOut.append("\n\nHistory:");
		for (int j = 0; j < histLen; j++){
			printOut.append("\nTradeId: " + tradeIdArray.get(j))
			.append(";Symbol: " + symbolArray.get(j))
			.append(";Quantity: " + qtyArray.get(j))
			.append(";Trade Status: " + tradeStatusArray.get(j))
			.append(";Trade Timestamp: " + histDtsArray.get(j));
		}
		return printOut.toString();
	}
}
