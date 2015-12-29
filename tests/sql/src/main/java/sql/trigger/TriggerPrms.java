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

public class TriggerPrms extends BasePrms {
	
	static {
		BasePrms.setValues(TriggerPrms.class);
	}
	
	public static Long pauseSec;	
	public static Long audit;
	public static Long triggerStmts;
	public static Long dropTriggerStmts;
	public static Long procedureStmts;
	
	public static String[] getTriggerStmts() {
		Vector statements = TestConfig.tab().vecAt(TriggerPrms.triggerStmts, new HydraVector());
		String[] strArr = new String[statements.size()];
		for (int i = 0; i < statements.size(); i++) {
			strArr[i] = (String)statements.elementAt(i);    
		} 
		return strArr;
	}
	
	public static String[] getDropTriggerStmts() {
		Vector statements = TestConfig.tab().vecAt(TriggerPrms.dropTriggerStmts, new HydraVector());
		String[] strArr = new String[statements.size()];   
		for (int i = 0; i < statements.size(); i++) {
			strArr[i] = (String)statements.elementAt(i);    
		} 
		return strArr;
	}
	
	public static String[] getProcedureStmts() {
		Vector statements = TestConfig.tab().vecAt(TriggerPrms.procedureStmts, new HydraVector());
		String[] strArr = new String[statements.size()];   
		for (int i = 0; i < statements.size(); i++) {
			strArr[i] = (String)statements.elementAt(i);    
		} 
		return strArr;
	}
}
