/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.TriggerEvent

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.impl.sql.execute;

/**
 * This is a simple class that we use to track
 * trigger events.  This is not expected to
 * be used directly, instead there is a static
 * TriggerEvent in TriggerEvents for each event 
 * found in this file.
 * 
 */
public class TriggerEvent
{
  // Gemstone changes BEGIN
	public static final int BEFORE_INSERT = 0;	
	public static final int BEFORE_DELETE = 1;	
	public static final int BEFORE_UPDATE = 2;	
	public static final int LAST_BEFORE_EVENT = BEFORE_UPDATE;	
	public static final int AFTER_INSERT = 3;	
	public static final int AFTER_DELETE = 4;	
	public static final int AFTER_UPDATE = 5;	
	public static final int MAX_EVENTS = 6;
  // Gemstone changes END
	private static final String Names[] = {	"BEFORE INSERT",
											"BEFORE DELETE", 
											"BEFORE UPDATE", 
											"AFTER INSERT", 
											"AFTER DELETE", 
											"AFTER UPDATE"
										};

	private boolean before;
	private int type;

	/**
	 * Create a trigger event of the given type
	 *
 	 * @param type the type
	 */
	TriggerEvent(int type)
	{
		this.type = type;
		switch(type)
		{
			case BEFORE_INSERT:		
			case BEFORE_DELETE:		
			case BEFORE_UPDATE:		
				before = true;
				break;

			case AFTER_INSERT:		
			case AFTER_DELETE:		
			case AFTER_UPDATE:		
				before = false;
				break;
		}
	}

	/**
	 * Get the type number of this trigger
 	 *
 	 * @return the type number
	 */
	int getNumber()
	{
		return type;
	}

	/**
	 * Get the type number of this trigger
 	 *
 	 * @return the type number
	 */
	String getName()
	{
		return Names[type];
	}

	/**
	 * Is this a before trigger
 	 *
 	 * @return true if before
	 */
	boolean isBefore()
	{
		return before;
	}

	/**
	 * Is this an after trigger
 	 *
 	 * @return true if after
	 */
	boolean isAfter()
	{
		return !before;
	}
}	
