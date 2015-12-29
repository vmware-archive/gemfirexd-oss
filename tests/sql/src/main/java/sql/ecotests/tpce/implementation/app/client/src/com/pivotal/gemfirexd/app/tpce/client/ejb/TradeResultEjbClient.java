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

import com.pivotal.gemfirexd.app.tpce.input.TradeResultTxnInput;
import com.pivotal.gemfirexd.app.tpce.jpa.ejb.EjbTradeResult;
import com.pivotal.gemfirexd.app.tpce.jpa.ejb.EjbTradeResultRemote;
import com.pivotal.gemfirexd.app.tpce.output.TradeResultTxnOutput;

public class TradeResultEjbClient {
    public static final String APP_NAME = "";
    public static final String MODULE_NAME = "ejb-jpa";
    public static final String BEAN_NAME =EjbTradeResult.class.getSimpleName();
    public static final String VIEWCLASS_NAME = EjbTradeResultRemote.class.getName();

    //TO Do: It is TradeOrder and should be converted to TradeResult,
    public static void main(String[] args) {
        try {
            TradeResultTxnInput trInput = new TradeResultTxnInput();
            trInput.setAcctId(43000007781L);
            trInput.setExecFirstName("Zora");
            trInput.setExecLastName("Chaddlesone");
            trInput.setExecTaxId("656LH4936SE875");
            trInput.setSymbol("ZGENPRA");
            trInput.setIssue("COMMON");
            trInput.setTradeTypeId("TLS");
            trInput.setStPendingId("PNDG");
            trInput.setRequestedPrice(27.58);
            trInput.setIsLifo(0);
            trInput.setTradeQty(800);
            //trInput.setRollItBack(1);
            EjbTradeResultRemote tr = lookupTradeResult();
            TradeResultTxnOutput trOutput = (TradeResultTxnOutput)tr.runTxn(trInput);
            System.out.println(trOutput.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static EjbTradeResultRemote lookupTradeResult() throws NamingException {
        final Hashtable<Object, Object> jndiProperties = new Hashtable<Object, Object>();

        jndiProperties.put(Context.URL_PKG_PREFIXES, "org.jboss.ejb.client.naming");
        final Context context = new InitialContext(jndiProperties);

        //EJB 3.1 java:global... seems not work
        String jndiString = "ejb:" + APP_NAME + "/" + MODULE_NAME + "/" + BEAN_NAME + "!" + VIEWCLASS_NAME;
        return (EjbTradeResultRemote) context.lookup(jndiString);
    }
}
