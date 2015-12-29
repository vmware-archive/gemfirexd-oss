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
package com.pivotal.gemfirexd.app.tpce.input;

import java.util.ArrayList;
import java.util.List;

import com.pivotal.gemfirexd.app.tpce.TPCETxnInput;

/**
 * The transaction Broker-Volume input
 * TPC-E Section 3.3.1
 */

public class BrokerVolumeTxnInput implements TPCETxnInput {
	private static final long serialVersionUID = 1L;
    private List<String> brokerList = new ArrayList<String>();
    private String sectorName;
    
    public BrokerVolumeTxnInput(String sectorName, List<String> brokerList){
    	this.sectorName = sectorName;
    	this.brokerList = brokerList;
        //TPCEConstants.MAX_BROKER_LIST_LEN;
    }

    public  List<String> getBrokerList(){
        return brokerList;
    }

    public void setBrokerList(List<String> brokerList){
        this.brokerList = brokerList;
    }
    
    public String getSectorName(){
        return sectorName;
    }
    
    public void setSectorName(String sectorName){
        this.sectorName = sectorName;
    }
}
