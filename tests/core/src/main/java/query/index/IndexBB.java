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

package query.index;


import hydra.blackboard.Blackboard;

//import com.gemstone.gemfire.cache.*;

public class IndexBB extends Blackboard {

	// Blackboard creation variables
	static String INDEX_BB_NAME = "Index_Blackboard";
	static String INDEX_BB_TYPE = "RMI";

	// singleton instance of blackboard
	static public IndexBB bbInstance = null;

	static String INDEX_VALIDATOR = "IndexValidator";

	static String COUNT_STAR_RESULTS = "counStartResults";
	
	// Various counters for index size and updates validations.
	public static int NUM_PR_COMPACTRANGE_ID_INDEX_VALUES;
	public static int NUM_PR_COMPACTRANGE_ID_INDEX_KEYS;
	public static int NUM_PR_RANGE_ID_INDEX_VALUES;
	public static int NUM_PR_RANGE_ID_INDEX_KEYS;
	public static int NUM_PR_COMPACTRANGE_STATUS_INDEX_VALUES;
	public static int NUM_PR_RANGE_STATUS_INDEX_VALUES;
	public static int NUM_PR_INDEX_KEYS;
	public static int NUM_PR_COMPACTRANGE_ID_INDEX_NUMBUCKETS;
	public static int NUM_PR_RANGE_ID_INDEX_NUMBUCKETS;
	public static int NUM_PR_COMPACTRANGE_STATUS_INDEX_NUMBUCKETS;
	public static int NUM_PR_RANGE_STATUS_INDEX_NUMBUCKETS;
	
	public static int TOTAL_USES;
	public static int TOTAL_USES_STATS;
	public static int TOTAL_UPDATES_STATS;
        
	/**
	 * Get the QueryBB
	 */
	public static IndexBB getBB() {
		if (bbInstance == null) {
			synchronized (IndexBB.class) {
				if (bbInstance == null)
					bbInstance = new IndexBB(INDEX_BB_NAME, INDEX_BB_TYPE);
			}
		}
		return bbInstance;
	}

	/**
	 * Initialize the QueryBB This saves caching attributes in the blackboard that
	 * must only be read once per test run.
	 */
	public static void HydraTask_initialize() {
		hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
		aMap.put(INDEX_VALIDATOR, new IndexValidator());
	}

	public static void putIndexValidator(IndexValidator iv) {
		hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
		aMap.put(INDEX_VALIDATOR, iv);
	}

	public static IndexValidator getIndexValidatorObject() {
		return ((IndexValidator) (IndexBB.getBB().getSharedMap()
				.get(IndexBB.INDEX_VALIDATOR)));
	}

	// Zero-arg constructor for remote method invocations.
	public IndexBB() {
	}

	/**
	 * Creates a sample blackboard using the specified name and transport type.
	 */
	public IndexBB(String name, String type) {
		super(name, type, IndexBB.class);
	}

}
