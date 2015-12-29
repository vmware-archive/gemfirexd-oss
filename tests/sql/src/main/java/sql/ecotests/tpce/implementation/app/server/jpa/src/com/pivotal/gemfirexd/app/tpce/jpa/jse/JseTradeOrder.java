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
package com.pivotal.gemfirexd.app.tpce.jpa.jse;

import com.pivotal.gemfirexd.app.tpce.TPCETxnOutput;
import com.pivotal.gemfirexd.app.tpce.input.TradeOrderTxnInput;
import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTradeOrder;
import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTxnManager;

public class JseTradeOrder extends JpaTradeOrder {
	public JseTradeOrder() {
		JseJpaTxnManager jseJpaTxnManager = new JseJpaTxnManager();
    	entityManager = jseJpaTxnManager.getEntityManager();
    	jpaTxnManager = (JpaTxnManager) jseJpaTxnManager;
	}
	
    public static void main(String[] args ) {
    	TradeOrderTxnInput toInput = new TradeOrderTxnInput(); 
    	toInput.setAcctId(43000001193L);  //James Petrak TaxId: 037ER7780YL262: c_id 4300000120
    	toInput.setExecFirstName("Maritza");
    	toInput.setExecLastName("Mccalister");
    	toInput.setExecTaxId("988XC3677NF760");
    	toInput.setSymbol("ADPT");
    	//toInput.setSymbol("COMS");
    	toInput.setIssue("COMMON");
    	toInput.setTradeTypeId("TMS");
    	toInput.setStSubmittedId("SBMT");
    	toInput.setIsLifo(1);
    	toInput.setTradeQty(50);
    	JseTradeOrder to = new JseTradeOrder();
    	TPCETxnOutput toOutput = to.runTxn(toInput);
    	System.out.println(toOutput.toString());
    }
}

