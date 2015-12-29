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
package com.gemstone.gemfire.management.internal;


/**
 * Abstract class containing methods which will be accesed 
 * by both Local and Remote filter chains
 * 
 * @author rishim
 *
 */
public abstract class FilterChain {

	protected boolean isFiltered(boolean included, boolean excluded) {

		if (excluded && included) {
			return true;
		}
		if (!excluded && included) {
			return false;
		}
		if (excluded && !included) {
			return true;
		}
		if (!excluded && !included) {
			return false;
		}
		return false;
	}
	
	/**
	 * This method splits the specified filters to array of string objects
	 */
	protected FilterParam createFilterParam(String inclusionList, String exclusionList){
		
		String[] inclusionListArray = inclusionList.split(";");
		String[] exclusionListArray = exclusionList.split(";");
		return new FilterParam(inclusionListArray, exclusionListArray);
	}
	
	


}
