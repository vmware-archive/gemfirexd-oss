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
package com.pivotal.gemfirexd.app.tpce.client.ejb;

import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import com.pivotal.gemfirexd.app.tpce.input.TradeOrderTxnInput;
import com.pivotal.gemfirexd.app.tpce.jpa.ejb.EjbTradeOrder;
import com.pivotal.gemfirexd.app.tpce.jpa.ejb.EjbTradeOrderRemote;
import com.pivotal.gemfirexd.app.tpce.output.TradeOrderTxnOutput;

public class TradeOrderEjbClient {
    public static final String APP_NAME = "";
    public static final String MODULE_NAME = "ejb-jpa";
    public static final String BEAN_NAME =EjbTradeOrder.class.getSimpleName();
    public static final String VIEWCLASS_NAME = EjbTradeOrderRemote.class.getName();

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
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
	    	EjbTradeOrderRemote to = lookupTradeOrder();
	    	TradeOrderTxnOutput toOutput = (TradeOrderTxnOutput)to.runTxn(toInput);
	    	System.out.println(toOutput.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

    private static EjbTradeOrderRemote lookupTradeOrder() throws NamingException {
        final Hashtable<Object, Object> jndiProperties = new Hashtable<Object, Object>();
               
        jndiProperties.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
        final Context context = new InitialContext(jndiProperties);

        //EJB 3.1 java:global... seems not work
        String jndiString = "ejb:" + APP_NAME + "/" + MODULE_NAME + "/" + BEAN_NAME + "!" + VIEWCLASS_NAME;
        return (EjbTradeOrderRemote) context.lookup(jndiString);
    }
}
