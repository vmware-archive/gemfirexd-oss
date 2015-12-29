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

package hydra;

import hydra.HadoopDescription.NodeDescription;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** 
 * Manages the state of Hadoop processes started by hydra clients using the
 * {@link HadoopHelper} API.
 */
public class HadoopMgr {

  /**
   * The current master map of created hadoop records.
   */
  private static Map<String, HadoopRecord> HadoopRecords = new HashMap();

   public static synchronized void removeHadoop(HadoopInfo info)
   throws RemoteException {
     HadoopRecord hr = HadoopRecords.get(info.getNodeDescription().getName());
     removeHadoop(info.getHadoopDescription(), info.getNodeDescription(),
                  hr.getPID(), hr.getSecure());
   }

   public static synchronized void recordHadoop(HadoopDescription hdd,
          NodeDescription nd, int pid, boolean secure) throws RemoteException {
    recordPID(hdd, nd, pid, secure);
    Nuker.getInstance().recordHDFSPIDNoDumps(nd.getHostDescription(), pid,
                                             secure);
   }

   public static synchronized void removeHadoop(HadoopDescription hdd,
          NodeDescription nd, int pid, boolean secure) throws RemoteException {
    removePID(hdd, nd, pid, secure);
    Nuker.getInstance().removeHDFSPIDNoDumps(nd.getHostDescription(), pid,
                                             secure);
  }

  /**
   * Records the information in the Hadoop record.
   */
  private static synchronized void recordPID(HadoopDescription hdd,
    NodeDescription nd, int pid, boolean secure) {

    HadoopRecord hr = HadoopRecords.get(nd.getName());
    if (hr == null) {
      hr = new HadoopRecord(hdd, nd, pid, secure);
      HadoopRecords.put(nd.getName(), hr);
    } else {
      hr.setPID(pid);
    }
  }

  /**
   * Removes the PID information from the Hadoop record.
   */
  private static synchronized void removePID(HadoopDescription hdd,
    NodeDescription nd, int pid, boolean secure) {

    HadoopRecord hr = HadoopRecords.get(nd.getName());
    if (hr == null) {
      String s = "No hadoop record found for " + nd.getName();
      throw new HydraInternalException(s);
    } else {
      hr.unsetPID(pid);
    }
  }

  /**
   * Returns info for Hadoop processes on the given host, optionally live only.
   */
  protected static synchronized List<HadoopInfo> getHadoopInfos(String host,
                                                 boolean liveOnly) {
    List<HadoopInfo> infos = new ArrayList();
    for (HadoopRecord hr : HadoopRecords.values()) {
      if (hr.getNodeDescription().getHostName().equals(host)) {
        if (liveOnly) {
          if (hr.isLive()) {
            infos.add(new HadoopInfo(hr.getHadoopDescription(),
                                     hr.getNodeDescription()));
          }
        } else {
          infos.add(new HadoopInfo(hr.getHadoopDescription(),
                                   hr.getNodeDescription()));
        }
      }
    }
    return infos;
  }
}
