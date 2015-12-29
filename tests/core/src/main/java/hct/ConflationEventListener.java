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

package hct;


import hydra.ProcessMgr;
import util.EventCountersBB;
import com.gemstone.gemfire.cache.EntryEvent;

/**
 * This event listener is used when conflation is enabled.It will update the value received for a given
 * key in EventCounterBB for that client.It also increments the counter for after update event.
 *
 * @author Suyog Bhokare
 *  
 */
public class ConflationEventListener extends EventListener {
	
	private static String CLIENT_NAME = "Client_" + ProcessMgr.getProcessId();		
	
	public ConflationEventListener(){
		
	}
	
	public void afterUpdate(EntryEvent event) {	 
	    logCall("afterUpdate", event);		
		String key  = CLIENT_NAME + "_" + event.getKey();
		EventCountersBB.getBB().getSharedMap().put(key,event.getNewValue());  	  
	    incrementAfterUpdateCounters(event, EventCountersBB.getBB());
	    checkVM();
	    checkCallback(event, BridgeNotify.updateCallbackPrefix);	   
	}
	
}
