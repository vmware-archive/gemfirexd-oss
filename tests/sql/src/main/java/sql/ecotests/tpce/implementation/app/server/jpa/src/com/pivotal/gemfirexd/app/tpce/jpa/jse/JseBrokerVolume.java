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

import java.util.ArrayList;
import java.util.List;

import com.pivotal.gemfirexd.app.tpce.input.BrokerVolumeTxnInput;
import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaBrokerVolume;
import com.pivotal.gemfirexd.app.tpce.output.BrokerVolumeTxnOutput;
import com.pivotal.gemfirexd.app.tpce.jpa.txn.JpaTxnManager;

public class JseBrokerVolume extends JpaBrokerVolume{
	public JseBrokerVolume() {
		JseJpaTxnManager jseJpaTxnManager = new JseJpaTxnManager();
    	entityManager = jseJpaTxnManager.getEntityManager();
    	jpaTxnManager = (JpaTxnManager) jseJpaTxnManager;
	}
		
    public static void main(String[] args ) {
    	List<String> brokerList = new ArrayList<String>();
    	brokerList.add("Sylvia P. Stieff");
    	brokerList.add("Mabel G. Clawson");
    	brokerList.add("Patrick G. Coder");
    	brokerList.add("Sylvia P. Stieff");
    	BrokerVolumeTxnInput bvInput = new BrokerVolumeTxnInput("Technology", brokerList); 
    	JseBrokerVolume bv = new JseBrokerVolume();
    	BrokerVolumeTxnOutput bvOutput = (BrokerVolumeTxnOutput)bv.runTxn(bvInput); 
    	System.out.println(bvOutput.toString());
    }
}
