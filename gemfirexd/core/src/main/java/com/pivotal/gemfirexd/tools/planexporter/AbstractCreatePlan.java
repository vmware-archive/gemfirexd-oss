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
package com.pivotal.gemfirexd.tools.planexporter;

import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
import java.sql.SQLException;
import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil.XMLForms;

/**
 * Abstract base class for different type of plan format creation.
 * 
 * @author soubhikc
 * 
 */
public abstract class AbstractCreatePlan {

  protected final AccessDistributedSystem ds;
  
  protected final XMLForms xmlForm;
  
  protected final String embedXslFileName;

  protected AbstractCreatePlan(final AccessDistributedSystem access,
      final boolean isRemote, XMLForms xmlForm, String embedXslFileName) {
    this.ds = access;
    if (isRemote) {
      ds.markRemote();
    }
    this.xmlForm = xmlForm;
    this.embedXslFileName = embedXslFileName;
  }

  protected ExecutionPlanMessage sendMessage() throws StandardException, SQLException,
      IOException {
    LanguageConnectionContext lcc = ds.getLanguageConnectionContext();

    Set<DistributedMember> members = GemFireXDUtils.getGfxdAdvisor()
        .adviseOperationNodes(null);

    final InternalDistributedMember myId = Misc.getGemFireCache().getMyId();
    members.remove(myId);

    final ExecutionPlanMessage msg = new ExecutionPlanMessage(
        lcc.getCurrentSchemaName(), ds.getQueryID(), xmlForm,
        embedXslFileName, Misc.getGemFireCache().getDistributedSystem(),
        members);

    // local plan first.
    CharArrayWriter out = new CharArrayWriter();
    try {
      processPlan(out, false);

      char[] lc = out.toCharArray();

      if (lc != null && lc.length > 0) {
        if (GemFireXDUtils.TracePlanGeneration) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION, this
              + " adding local result " + String.valueOf(lc));
        }
        msg.addResult(lc);
      }
    } finally {
      out.close();
    }

    if (members.size() > 0) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
          "sending execution plan message for " + ds.getQueryID());
      }
      InternalDistributedSystem ids = Misc.getDistributedSystem();
      msg.send(ids, ids.getDM(),
          msg.getReplyProcessor(), true, false);

      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "processing results from remote nodes for " + ds.getQueryID());
      }
    }
      
    return msg;
  }

  abstract void processPlan(CharArrayWriter out, boolean isLocalPlanExtracted)
      throws SQLException, IOException;
}
