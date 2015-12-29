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

package perffmwk;

import java.rmi.*;
import java.util.*;

/**
 * This is the interface for the remote {@link perffmwk.RemotePerfStatMgrImpl} object.
 * Test code should not invoke this object directly.  Instead, it should use
 * {@link perffmwk.PerfStatMgr#getInstance()}.
 *
 * @see PerfStatMgr
 */
public interface RemotePerfStatMgr extends Remote {

  /**
   * See {@link perffmwk.PerfStatMgr#startTrim(String)} for
   * application usage.
   */
  public void startTrim( String trimspecName )
    throws RemoteException;

  /**
   * See {@link perffmwk.PerfStatMgr#endTrim(String)} for
   * application usage.
   */
  public void endTrim( String trimspecName )
    throws RemoteException;

  /**
   * See {@link perffmwk.PerfStatMgr#reportTrimInterval(TrimInterval)} for
   * application usage.
   */
  public void reportTrimInterval(TrimInterval interval)
    throws RemoteException;

  /**
   * See {@link perffmwk.PerfStatMgr#reportExtendedTrimInterval(TrimInterval)} for
   * application usage.
   */
  public void reportExtendedTrimInterval(TrimInterval interval)
    throws RemoteException;

  /**
   * See {@link perffmwk.PerfStatMgr#reportTrimIntervals(Map)} for
   * application usage.
   */
  public void reportTrimIntervals( Map intervals )
    throws RemoteException;

  /**
   * See {@link perffmwk.PerfStatMgr#reportExtendedTrimIntervals(Map)} for
   * application usage.
   */
  public void reportExtendedTrimIntervals( Map intervals )
    throws RemoteException;

  /**
   * See <code>perffmwk.PerfStatMgr#getTrimSpec(String)</code> for
   * application usage.
   */
  public TrimSpec getTrimSpec( String trimspecName )
    throws RemoteException;

  /**
   * See {@link perffmwk.PerfStatMgr#registerStatistics(PerformanceStatistics)}.
   */
  public void registerStatistics( List statSpecs )
    throws RemoteException;
}
