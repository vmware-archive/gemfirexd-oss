/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.RowTriggerExecutor

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

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SPSDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TriggerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.CursorResultSet;

/**
 * A row trigger executor is an object that executes
 * a row trigger.  It is instantiated at execution time.
 * There is one per row trigger.
 */
class RowTriggerExecutor extends GenericTriggerExecutor
{
	/**
	 * Constructor
	 *
	 * @param tec the execution context
	 * @param triggerd the trigger descriptor
	 * @param activation the activation
	 * @param lcc the lcc
	 */
	RowTriggerExecutor
	(
		InternalTriggerExecutionContext tec, 
		TriggerDescriptor 				triggerd,
		Activation						activation,
		LanguageConnectionContext		lcc
	)
	{
		super(tec, triggerd, activation, lcc);
	}

	/**
	 * Fire the trigger based on the event.
	 *
	 * @param event the trigger event
	 * @param brs   the before result set
	 * @param ars   the after result set
	 *
	 * @exception StandardExcetion on error or general trigger
	 *	exception
	 */
	void fireTrigger 
	(
		TriggerEvent 		event, 
		CursorResultSet 	brs, 
		CursorResultSet 	ars
	) throws StandardException
	{
	  // Gemstone changes BEGIN
//		tec.setTrigger(triggerd);
//		
//		try
//		{
//			while (true)
//			{
//				if (brs != null)
//				{
//					if (brs.getNextRow() == null)	
//						break;
//				}
//	
//				if (ars != null)
//				{
//					if (ars.getNextRow() == null)	
//						break;
//				}
//	
//				tec.setBeforeResultSet(brs == null ? 
//						null : 
//						TemporaryRowHolderResultSet.
//									   getNewRSOnCurrentRow(activation, brs));
//					
//				tec.setAfterResultSet(ars == null ? 
//									  null : 
//									  TemporaryRowHolderResultSet.
//									  getNewRSOnCurrentRow(activation, ars));
//
//				/* 	
//					This is the key to handling autoincrement values that might
//					be seen by insert triggers. For an AFTER ROW trigger, update
//					the autoincrement counters before executing the SPS for the
//					trigger.
//				*/
//				if (event.isAfter()) 
//					tec.updateAICounters();
//
//				executeSPS(getAction());
//				
//				/*
//				  For BEFORE ROW triggers, update the ai values after the SPS
//				  has been executed. This way the SPS will see ai values from
//				  the previous row.
//				*/
//				if (event.isBefore())
//					tec.updateAICounters();
//			}
//		} 
//		finally
//		{
//			clearSPS();
//			tec.clearTrigger();
//		}
		return;
	// Gemstone changes BEGIN
	}
}
