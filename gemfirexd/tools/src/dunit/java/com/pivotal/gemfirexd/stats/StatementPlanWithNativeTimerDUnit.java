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
package com.pivotal.gemfirexd.stats;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.pivotal.gemfirexd.TestUtil;

/**
 * Previously, we were using Native Timer with CLOCKID_PROCESS_CPUTIME_ID for
 * explain plans. But this is costly operation and hence we moved to
 * System.nanotime. But still, the user can set property to enable native timer
 * instead of java timer and hence this test.
 * 
 * @author hemantb
 *
 */
@SuppressWarnings("serial")
public class StatementPlanWithNativeTimerDUnit extends StatementPlanDUnit {

  public StatementPlanWithNativeTimerDUnit(String name) {
    super(name);
  }

  public void setNativeNanoTimer() throws SQLException {
    Connection conn = TestUtil.getConnection();
    CallableStatement st = conn
        .prepareCall("CALL SYS.SET_NANOTIMER_TYPE(?, ?)");
    st.setBoolean(1, true);
    st.setString(2, "CLOCK_PROCESS_CPUTIME_ID");
    st.execute();
  }

  public void testNanoTimerSPs() throws Exception {
    startVMs(1, 3);
    Connection conn = TestUtil.getConnection();

    ResultSet rs = conn.createStatement()
        .executeQuery("values SYS.GET_NATIVE_NANOTIMER_TYPE()");
    assertTrue(rs.next());
    String defaultTimeType = rs.getString(1);
    assertFalse(rs.next());

    setNativeNanoTimer();

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "values SYS.GET_IS_NATIVE_NANOTIMER()", null, "true");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "values SYS.GET_NATIVE_NANOTIMER_TYPE()", null,
        "CLOCK_PROCESS_CPUTIME_ID");

    CallableStatement st = conn
        .prepareCall("CALL SYS.SET_NANOTIMER_TYPE(?, ?)");
    st.setBoolean(1, true);
    st.setString(2, "CLOCK_REALTIME");
    st.execute();

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "values SYS.GET_IS_NATIVE_NANOTIMER()", null, "true");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "values SYS.GET_NATIVE_NANOTIMER_TYPE()", null, "CLOCK_REALTIME");

    // check reset to default
    st = conn.prepareCall("CALL SYS.SET_NANOTIMER_TYPE(?, ?)");
    st.setBoolean(1, true);
    st.setString(2, "DEFAULT");
    st.execute();

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "values SYS.GET_IS_NATIVE_NANOTIMER()", null, "false");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "values SYS.GET_NATIVE_NANOTIMER_TYPE()", null, defaultTimeType);

    // check reset to non-native default MONOTONIC type
    st = conn.prepareCall("CALL SYS.SET_NANOTIMER_TYPE(?, ?)");
    st.setBoolean(1, false);
    st.setString(2, "CLOCK_THREAD_CPUTIME_ID");
    st.execute();

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "values SYS.GET_IS_NATIVE_NANOTIMER()", null, "false");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3 },
        "values SYS.GET_NATIVE_NANOTIMER_TYPE()", null, "CLOCK_MONOTONIC");
  }
}
