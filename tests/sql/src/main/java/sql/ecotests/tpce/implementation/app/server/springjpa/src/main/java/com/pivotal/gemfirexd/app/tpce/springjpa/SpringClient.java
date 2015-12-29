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
package com.pivotal.gemfirexd.app.tpce.springjpa;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.pivotal.gemfirexd.app.tpce.TPCETxnInterface;
import com.pivotal.gemfirexd.app.tpce.TPCETxnOutput;
import com.pivotal.gemfirexd.app.tpce.input.TradeOrderTxnInput;

public class SpringClient {

	private static ApplicationContext ctx = null;
		
	public static void main(String[] args) {
		ctx = new ClassPathXmlApplicationContext("tpce-applicationContext.xml");
		
		//System.out.println("===Customer Position Transaction===:"); 
		//TPCETxnInterface cp = (TPCETxnInterface)ctx.getBean("springCustomerPosition");		
	    //CustomerPositionTxnInput cpInput = new CustomerPositionTxnInput(0, /*4300000762L*/0, 1, "170HQ9038TX397"/*""*/); 
	    /*
	    CustomerPositionTxnOutput cpOutput = (CustomerPositionTxnOutput)cp.runTxn(cpInput);
    	System.out.println(cpOutput.toString());
    	
    	System.out.println("\n===Broker View Transaction===:"); 
    	TPCETxnInterface bv = (TPCETxnInterface)ctx.getBean("springBrokerVolume");
    	List<String> brokerList = new ArrayList<String>();
    	brokerList.add("Sylvia P. Stieff");
    	brokerList.add("Mabel G. Clawson");
    	brokerList.add("Patrick G. Coder");
    	brokerList.add("Sylvia P. Stieff");
    	BrokerVolumeTxnInput bvInput = new BrokerVolumeTxnInput("Technology", brokerList); 
    	BrokerVolumeTxnOutput bvOutput = (BrokerVolumeTxnOutput)bv.runTxn(bvInput); 
    	System.out.println(bvOutput.toString());
        */
    	
    	System.out.println("\n===Trade Order Transaction===:");   
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
    	//toInput.setRollItBack(1);  	
    	TPCETxnInterface to = (TPCETxnInterface)ctx.getBean("springTradeOrder");
    	TPCETxnOutput toOutput = to.runTxn(toInput);
    	System.out.println(toOutput.toString());    	
	}

}
