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


/**
 * Hydra parameters for the ClientQueue tests.
 * It contains parameters for configuring both the cache server and
 * the the edge client VMs.
 *
 * @author Suyog Bhokare, Girish Thombare
 * @since danube
 */
public class ClientQueuePrms extends BridgeNotifyPrms {

	/**
	 *  Parameter to enable conflation. It true conflation enabled else disabled.
	 */
	 public static Long conflationEnabled;
	 
	 /**
	  * If conflation is enabled this variable is used for sleeping in client's cacheListener.
	  */
	 public static Long timeToSleep;
	 
	 /**
	  * If conflation is enabled then it is used to delay the thread after batch put.
	  */
	 public static Long timeToWaitForConflation;
	 
	 
	 static {
	       setValues( ClientQueuePrms.class );
	   }	
}
