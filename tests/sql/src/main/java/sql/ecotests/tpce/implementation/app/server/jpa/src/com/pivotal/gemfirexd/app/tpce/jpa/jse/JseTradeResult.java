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
import com.pivotal.gemfirexd.app.tpce.input.TradeResultTxnInput;
import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTradeResult;
import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTxnManager;

public class JseTradeResult extends JpaTradeResult {
    public JseTradeResult() {
        JseJpaTxnManager jseJpaTxnManager = new JseJpaTxnManager();
        entityManager = jseJpaTxnManager.getEntityManager();
        jpaTxnManager = (JpaTxnManager) jseJpaTxnManager;
    }

    //TO Do: It is TradeOrder and should be converted to TradeResult,
    public static void main(String[] args ) {
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
        JseTradeResult tr = new JseTradeResult();
        TPCETxnOutput trOutput = tr.runTxn(trInput);
        System.out.println(trOutput.toString());
    }
}

