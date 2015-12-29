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
/**
 * 
 */
package com.pivotal.gemfirexd.app.tpce.output;

import java.util.ArrayList;
import java.util.List;

import com.pivotal.gemfirexd.app.tpce.TPCETxnOutput;

public class BrokerVolumeTxnOutput implements TPCETxnOutput {
	private static final long serialVersionUID = 1L;
	
	private int listLen = 0;
	private int status = 0;
	private List<Double> volumeArray = new ArrayList<Double>();
	
	public int getListLen() {
		return listLen;
	}
	 
	public void setListLen(int listLen) {
		this.listLen = listLen;
	}
	
	public int getStatus() {
		return status;
	}
	 
	public void setStatus(int status) {
		this.status = status;
	}	
	
	public List<Double> getVolumeArray() {
		return volumeArray;
	}
	 
	public void setVolumeArray(List<Double> volumeArray) {
		this.volumeArray = volumeArray;
	}	
	
	public String toString(){
		return "Status: " + status
				+ "\nVolume List Count: " + listLen
				+ "\nVolumes: " + volumeArray.toString();
	}
}
