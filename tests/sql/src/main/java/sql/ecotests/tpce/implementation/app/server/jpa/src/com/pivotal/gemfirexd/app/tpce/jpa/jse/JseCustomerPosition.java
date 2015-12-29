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


import com.pivotal.gemfirexd.app.tpce.input.CustomerPositionTxnInput;

import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaCustomerPosition;
import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTxnManager;
import com.pivotal.gemfirexd.app.tpce.output.CustomerPositionTxnOutput;

/**
 * The Java class for the transaction Customer-Position.
 * TPC-E Section 3.3.2
 */

public class JseCustomerPosition extends JpaCustomerPosition {	
	public JseCustomerPosition() {
		JseJpaTxnManager jseJpaTxnManager = new JseJpaTxnManager();
    	entityManager = jseJpaTxnManager.getEntityManager();
    	jpaTxnManager = (JpaTxnManager) jseJpaTxnManager;
	}
	
    public static void main(String[] args ) {
    	CustomerPositionTxnInput cpInput = new CustomerPositionTxnInput(0, /*4300000762L*/0, 1, "170HQ9038TX397"/*""*/); 
    	JseCustomerPosition cp = new JseCustomerPosition();
    	CustomerPositionTxnOutput cpOutput = (CustomerPositionTxnOutput)cp.runTxn(cpInput);
    	System.out.println(cpOutput.toString());   	
    }
}