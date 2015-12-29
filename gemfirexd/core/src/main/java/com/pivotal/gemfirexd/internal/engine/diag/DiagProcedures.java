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

package com.pivotal.gemfirexd.internal.engine.diag;

import java.sql.SQLException;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdListResultCollector;
import com.pivotal.gemfirexd.internal.engine.distributed.message.GfxdConfigMessage;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;

/**
 * Contains bodies of procedures used to enable/disable various diagnostic
 * related tasks.
 * 
 * @author swale
 */
public final class DiagProcedures {

  // to enable this class to be included in gemfirexd.jar
  public static void dummy() {
  }

  /**
   * Enable or disable collection of various query statistics.
   * 
   * @param enable
   *          true to enable collection of query statistics and false to disable
   */
  public static void setQueryStats(Boolean enable) throws StandardException,SQLException {
	// NULL enable  is illegal
	if (enable == null)
	{
		throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
	}
    try {
      GfxdConfigMessage<Object> msg = new GfxdConfigMessage<Object>(
          new GfxdListResultCollector(), null,
          GfxdConfigMessage.Operation.ENABLE_QUERYSTATS, enable, false);
      // don't need the result
      msg.executeFunction();
    } catch (SQLException ex) {
      throw ex;
    } catch (Throwable t) {
      throw Util.javaException(t);
    }
  }

  /**
   * Get the server groups of this VM.
   */
  public static String getServerGroups() throws SQLException {
    try {
      return ServerGroupUtils.getMyGroups();
    } catch (Throwable t) {
      throw Util.javaException(t);
    }
  }

  /**
   * Get the string representation of the {@link DistributedMember} of this VM.
   */
  public static String getDistributedMemberId() throws SQLException {
    try {
      return String.valueOf(Misc.getMyId());
    } catch (Throwable t) {
      throw Util.javaException(t);
    }
  }
}
