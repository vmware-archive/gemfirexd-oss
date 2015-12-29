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
package sustainability;

import hydra.blackboard.Blackboard;

public class SustainabilityBB extends Blackboard {
	
//	 Blackboard creation variables
	static String SUS_BB_NAME = "Sustainability_Blackboard";
	static String SUS_BB_TYPE = "RMI";
	private static SustainabilityBB susBB = null;
	/**
	 * Returns a singleton for this blackboard.
	 * 
	 * @return SustainabilityBB
	 * */
	public static SustainabilityBB getBB() {
		if (susBB == null) {
                   synchronized ( SustainabilityBB.class ) {
                      if (susBB == null) 
			   susBB =new SustainabilityBB(SUS_BB_NAME, SUS_BB_TYPE);
                   }
                }
		return susBB;
	}
	/**
	 * No argument constructor.
	 * */
	public SustainabilityBB() {
	}
	/**
	 * Constructor with name and type specified.
	 * 
	 * @return SustainabilityBB
	 * 
	 * */
	public SustainabilityBB(String name, String type) {
		super(name, type, SustainabilityBB.class);
	}
	
	//public hydra.blackboard.SharedMap getSharedMap(){
	//	return susBB.getSharedMap();
	//}

}
