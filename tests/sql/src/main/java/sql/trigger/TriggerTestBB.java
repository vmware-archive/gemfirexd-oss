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
package sql.trigger;

import java.util.*;
import hydra.*;

import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedCounters;

public class TriggerTestBB extends Blackboard {
	
	public static String NAME = "TriggerTest_Blackboard";
	public static String TYPE = "RMI";
	public static int NUM_INSERT;
	public static int NUM_PROC;
	
	public TriggerTestBB() {}
	
	public TriggerTestBB(String name, String type){
		super(name, type, TriggerTestBB.class);
	}
	
	public static TriggerTestBB getBB(){
		return new TriggerTestBB(NAME,TYPE);
	}
	
}

