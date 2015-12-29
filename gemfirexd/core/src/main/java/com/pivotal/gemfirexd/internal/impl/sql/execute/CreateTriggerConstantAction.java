/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.CreateTriggerConstantAction

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












import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Parser;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Dependent;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SPSDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TriggerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.impl.sql.compile.CompilerContextImpl;
import com.pivotal.gemfirexd.internal.impl.sql.compile.CreateTriggerNode.GfxdIMParamInfo;
import com.pivotal.gemfirexd.internal.impl.sql.compile.DMLModStatementNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StatementNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.TableName;

import java.sql.Timestamp;
import java.util.List;
import java.util.Vector;

/**
 * This class  describes actions that are ALWAYS performed for a
 * CREATE TRIGGER Statement at Execution time.  
 *
 */
//GemFire changes BEGIN
public class CreateTriggerConstantAction extends DDLSingleTableConstantAction
//GemFire changes END
{

	private String					triggerName;
	private String					triggerSchemaName;
	private TableDescriptor			triggerTable;		// null after readExternal
	private UUID					triggerTableId;		// set in readExternal
	private int						eventMask;
	private boolean					isBefore;
	private boolean					isRow;
	private boolean					isEnabled;
	private boolean					referencingOld;
	private boolean					referencingNew;
	private UUID					whenSPSId;
	private String					whenText;
	private UUID					actionSPSId;
	private String					actionText;
	private String					originalActionText;
	private String					oldReferencingName;
	private String					newReferencingName;
	private UUID					spsCompSchemaId;
	private Timestamp				creationTimestamp;
	private int[]					referencedCols;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a CREATE TRIGGER statement.
	 *
	 * @param triggerSchemaName	name for the schema that trigger lives in.
	 * @param triggerName	Name of trigger
	 * @param eventMask		TriggerDescriptor.TRIGGER_EVENT_XXXX
	 * @param isBefore		is this a before (as opposed to after) trigger 
	 * @param isRow			is this a row trigger or statement trigger
	 * @param isEnabled		is this trigger enabled or disabled
	 * @param triggerTable	the table upon which this trigger is defined
	 * @param whenSPSId		the sps id for the when clause (may be null)
	 * @param whenText		the text of the when clause (may be null)
	 * @param actionSPSId	the spsid for the trigger action (may be null)
	 * @param actionText	the text of the trigger action
	 * @param spsCompSchemaId	the compilation schema for the action and when
	 *							spses.   If null, will be set to the current default
	 *							schema
	 * @param creationTimestamp	when was this trigger created?  if null, will be
	 *						set to the time that executeConstantAction() is invoked
	 * @param referencedCols	what columns does this trigger reference (may be null)
	 * @param originalActionText The original user text of the trigger action
	 * @param referencingOld whether or not OLD appears in REFERENCING clause
	 * @param referencingNew whether or not NEW appears in REFERENCING clause
	 * @param oldReferencingName old referencing table name, if any, that appears in REFERENCING clause
	 * @param newReferencingName new referencing table name, if any, that appears in REFERENCING clause
	 */
	CreateTriggerConstantAction
	(
		String				triggerSchemaName,
		String				triggerName,
		int					eventMask,
		boolean				isBefore,
		boolean 			isRow,
		boolean 			isEnabled,
		TableDescriptor		triggerTable,
		UUID				whenSPSId,
		String				whenText,
		UUID				actionSPSId,
		String				actionText,
		UUID				spsCompSchemaId,
		Timestamp			creationTimestamp,
		int[]				referencedCols,
		String				originalActionText,
		boolean				referencingOld,
		boolean				referencingNew,
		String				oldReferencingName,
		String				newReferencingName
	)
	{
		super(triggerTable.getUUID());
		this.triggerName = triggerName;
		this.triggerSchemaName = triggerSchemaName;
		this.triggerTable = triggerTable;
		this.eventMask = eventMask;
		this.isBefore = isBefore;
		this.isRow = isRow;
		this.isEnabled = isEnabled;
		this.whenSPSId = whenSPSId;
		this.whenText = whenText;
		this.actionSPSId = actionSPSId;
		this.actionText = actionText;
		this.spsCompSchemaId = spsCompSchemaId;
		this.creationTimestamp = creationTimestamp;
		this.referencedCols = referencedCols;
		this.originalActionText = originalActionText;
		this.referencingOld = referencingOld;
		this.referencingNew = referencingNew;
		this.oldReferencingName = oldReferencingName;
		this.newReferencingName = newReferencingName;
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(triggerSchemaName != null, "triggerSchemaName sd is null");
			SanityManager.ASSERT(triggerName != null, "trigger name is null");
			SanityManager.ASSERT(triggerTable != null, "triggerTable is null");
			SanityManager.ASSERT(actionText != null, "actionText is null");
		}
	}

	/**
	 * This is the guts of the Execution-time logic for CREATE TRIGGER.
	 *
	 * @see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction(Activation activation)
						throws StandardException
	{
		SPSDescriptor				whenspsd = null;
		//SPSDescriptor				actionspsd;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		/*
		** Indicate that we are about to modify the data dictionary.
		** 
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		SchemaDescriptor triggerSd = getSchemaDescriptorForCreate(dd, activation, triggerSchemaName);

		if (spsCompSchemaId == null) {
			SchemaDescriptor def = lcc.getDefaultSchema();
			if (def.getUUID() == null) {
				// Descriptor for default schema is stale,
				// look it up in the dictionary
				def = dd.getSchemaDescriptor(def.getDescriptorName(), tc, 
											 false);
			}
			
			/* 
			** It is possible for spsCompSchemaId to be null.  For instance, 
			** the current schema may not have been physically created yet but 
			** it exists "virtually".  In this case, its UUID will have the 
			** value of null meaning that it is not persistent.  e.g.:   
			**
			** CONNECT 'db;create=true' user 'ernie';
			** CREATE TABLE bert.t1 (i INT);
			** CREATE TRIGGER bert.tr1 AFTER INSERT ON bert.t1 
			**    FOR EACH STATEMENT MODE DB2SQL 
			**    SELECT * FROM SYS.SYSTABLES;
			**
			** Note that in the above case, the trigger action statement have a 
			** null compilation schema.  A compilation schema with null value 
			** indicates that the trigger action statement text does not have 
			** any dependencies with the CURRENT SCHEMA.  This means:
			**
			** o  It is safe to compile this statement in any schema since 
			**    there is no dependency with the CURRENT SCHEMA. i.e.: All 
			**    relevent identifiers are qualified with a specific schema.
			**
			** o  The statement cache mechanism can utilize this piece of 
			**    information to enable better statement plan sharing across 
			**    connections in different schemas; thus, avoiding unnecessary 
			**    statement compilation.
			*/ 
			if (def != null)
				spsCompSchemaId = def.getUUID();
		}

		String tabName;
		if (triggerTable != null)
		{
			triggerTableId = triggerTable.getUUID();
			tabName = triggerTable.getName();
		}
		else
			tabName = "with UUID " + triggerTableId;

		/* We need to get table descriptor again.  We simply can't trust the
		 * one we got at compile time, the lock on system table was released
		 * when compile was done, and the table might well have been dropped.
		 */
		triggerTable = dd.getTableDescriptor(triggerTableId);
		if (triggerTable == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tabName);
		}
// GemStone changes BEGIN
		// check for any open ResultSets
		lcc.verifyNoOpenResultSets(null, triggerTable,
		    DependencyManager.CREATE_TRIGGER);
// GemStone changes END
		/* Lock the table for DDL.  Otherwise during our execution, the table
		 * might be changed, even dropped.  Beetle 4269
		 */
		lockTableForDDL(tc, triggerTable.getHeapConglomerateId(), true);
		/* get triggerTable again for correctness, in case it's changed before
		 * the lock is aquired
		 */
		triggerTable = dd.getTableDescriptor(triggerTableId);
		if (triggerTable == null)
		{
			throw StandardException.newException(
								SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION,
								tabName);
		}

		/*
		** Send an invalidate on the table from which
		** the triggering event emanates.  This it
		** to make sure that DML statements on this table
		** will be recompiled.  Do this before we create
		** our trigger spses lest we invalidate them just
		** after creating them.
		*/
		dm.invalidateFor(triggerTable, DependencyManager.CREATE_TRIGGER, lcc);

		/*
		** Lets get our trigger id up front, we'll use it when
	 	** we create our spses.
		*/
		UUID tmpTriggerId = dd.getUUIDFactory().createUUID();

		actionSPSId = (actionSPSId == null) ? 
			dd.getUUIDFactory().createUUID() : actionSPSId;
 
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		/*
		** Create the trigger descriptor first so the trigger action
		** compilation can pick up the relevant trigger especially in 
		** the case of self triggering.
		*/
		TriggerDescriptor triggerd =
				ddg.newTriggerDescriptor(
									triggerSd,
									tmpTriggerId,
									triggerName,
									eventMask,
									isBefore,
									isRow,
									isEnabled,
									triggerTable,
									whenspsd == null ? null : whenspsd.getUUID(),
									actionSPSId,
									creationTimestamp == null ? new Timestamp(System.currentTimeMillis()) : creationTimestamp,
									referencedCols,
									originalActionText,
									referencingOld,
									referencingNew,
									oldReferencingName,
									newReferencingName);


		dd.addDescriptor(triggerd, triggerSd,
								DataDictionary.SYSTRIGGERS_CATALOG_NUM, false,
								tc);


		/*	
		** If we have a WHEN action we create it now.
		*/
// GemStone changes BEGIN
		// GemFireXD does not use the SPSDescriptors below. If this is
		// ever uncommented then need to design afresh for deadlocks
		// in SPSDescriptor compilations (see multiple issues around
		// this #47336, #45609 etc)
		/* (original code)
		if (whenText != null)
		{
			whenspsd = createSPS(lcc, ddg, dd, tc, tmpTriggerId, triggerSd,
						whenSPSId, spsCompSchemaId, whenText, true, triggerTable);
		}

		/*
		** Create the trigger action
		*
		actionspsd = createSPS(lcc, ddg, dd, tc, tmpTriggerId, triggerSd,
						actionSPSId, spsCompSchemaId, actionText, false, triggerTable);
		
		/*
		** Make underlying spses dependent on the trigger.
		*
		if (whenspsd != null)
		{
			dm.addDependency(triggerd, whenspsd, lcc.getContextManager());
		}
		dm.addDependency(triggerd, actionspsd, lcc.getContextManager());
		dm.addDependency(triggerd, triggerTable, lcc.getContextManager());
		dm.addDependency(actionspsd, triggerTable, lcc.getContextManager());
		*/
// GemStone changes END
		//store trigger's dependency on various privileges in the dependeny system
		storeViewTriggerDependenciesOnPrivileges(activation, triggerd);
		// Gemstone changes BEGIN
		CompilerContext cc = null;
		GfxdIndexManager indexManager = GfxdIndexManager.getGfxdIndexManager(triggerTable, lcc);
                if (lcc != null) {
                  try {
                    cc = (CompilerContext) (lcc.getContextManager().getContext(CompilerContext.CONTEXT_ID));
                    CompilerContextImpl cimpl = (CompilerContextImpl)cc;
                    Dependent origDependent = cimpl.getCurrentDependent();
                    cimpl.setCurrentDependent(triggerd);
                    final Parser p = cc.getParser();
                    StatementNode st = null;
                    try {
                     st = p.parseStatement(this.originalActionText);
                    }
                    finally {
                      cc.setCurrentDependent(origDependent);
                    }
                    if (st instanceof DMLModStatementNode) {
                      TableName targetTabName = ((DMLModStatementNode)st).getTargetTable();
                      LocalRegion targetRegion = (LocalRegion)Misc.getRegionForTableByPath(
                          targetTabName.getFullTableName(), false);
                      LocalRegion thisRegion = (LocalRegion)Misc.getRegionForTableByPath(
                          Misc.getRegionPath(triggerTable, lcc), true);
                      if (targetRegion == thisRegion) {
                        throw StandardException
                        .newException(SQLState.NOT_IMPLEMENTED,
                            "cannot define triggers such that it modifies itself");
                      }
                      if (targetRegion != null) {
                        GfxdIndexManager targetIM = (GfxdIndexManager)targetRegion
                            .getIndexUpdater();
                        if (targetIM.hasThisTableAsTarget(Misc.getRegionPath(triggerTable, lcc))) {
                          throw StandardException
                              .newException(SQLState.NOT_IMPLEMENTED,
                                  "two tables cannot have triggers such that they act on each other");
                        }
                        else {
                          indexManager.addTriggerTargetTableName(
                              targetTabName.getFullTableNameAsRegionPath());
                        }
                      }
                    }
                  } finally {
                    if (cc != null) {
                      lcc.popCompilerContext(cc);
                    }
                  }
                }
		//index manager null when accessor
		if (indexManager != null) {
		  indexManager.addTriggerExecutor(triggerd, this.gfxdIMActionText, this.gfxdIMparaminfovec);
		}
		// Gemstone changes END
	}


// GemStone changes BEGIN
	/* (original code)
	/*
	** Create an sps that is used by the trigger.
	*
	private SPSDescriptor createSPS
	(
		LanguageConnectionContext	lcc,
		DataDescriptorGenerator 	ddg,
		DataDictionary				dd,
		TransactionController		tc,
		UUID						triggerId,
		SchemaDescriptor			sd,
		UUID						spsId,
		UUID						compSchemaId,
		String						text,
		boolean						isWhen,
		TableDescriptor				triggerTable
	) throws StandardException	
	{
		if (text == null)
		{
			return null; 
		}

		/*
		** Note: the format of this string is very important.
		** Dont change it arbitrarily -- see sps code.
		*
		String spsName = "TRIGGER" + 
						(isWhen ? "WHEN_" : "ACTN_") + 
						triggerId + "_" + triggerTable.getUUID().toString();

		SPSDescriptor spsd = new SPSDescriptor(dd, spsName,
									(spsId == null) ?
										dd.getUUIDFactory().createUUID() :
										spsId,
									sd.getUUID(),
									compSchemaId == null ?
										lcc.getDefaultSchema().getUUID() :
										compSchemaId,
									SPSDescriptor.SPS_TYPE_TRIGGER,
									true,				// it is valid
									text,				// the text
									true );	// no defaults

		/*
		** Prepared the stored prepared statement
		** and release the activation class -- we
		** know we aren't going to execute statement
		** after create it, so for now we are finished.
		*
		spsd.prepareAndRelease(lcc, triggerTable);


		dd.addSPSDescriptor(spsd, tc, true);

		return spsd;
	}
	*/
// GemStone changes END

	public String toString()
	{
		return constructToString("CREATE TRIGGER ", triggerName);		
	}
// GemStone changes BEGIN

  @Override
  public final String getSchemaName() {
    return this.triggerTable.getSchemaName();
  }

  @Override
  public final String getTableName() {
    return this.triggerTable.getName();
  }

  @Override
  public final String getObjectName() {
    return this.triggerName;
  }
  
  // This will be used by the gfxd index manager to prepare 
  // the action prepared statement
  private String gfxdIMActionText;
  private List<GfxdIMParamInfo> gfxdIMparaminfovec;
  
  public void setGfxdActionText(String gfxdActionText) {
    this.gfxdIMActionText = gfxdActionText;
  }
  
  public void setParamInfos(List<GfxdIMParamInfo> gfxdparaminfovec) {
    this.gfxdIMparaminfovec = gfxdparaminfovec;  
  }
// GemStone changes END
}


