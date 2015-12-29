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

package hydra.gemfirexd;

import com.gemstone.gemfire.SystemFailure;
import hydra.DistributedSystemHelper;
import hydra.FileUtil;
import hydra.GemFireDescription;
import hydra.HostDescription;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.Nuker;
import hydra.RemoteTestModule;
import java.io.File;
import java.rmi.RemoteException;
import java.util.Properties;

/**
 * Provides support methods for GemFireXD.
 */
public class GfxdHelper {

    private static String diskDirName;

//------------------------------------------------------------------------------
// Connection properties
//------------------------------------------------------------------------------

  public static Properties getConnectionProperties() {
    GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
    Properties p = gfd.getDistributedSystemProperties();
    boolean persistDD = GfxdHelperPrms.persistDD();
    boolean persistQueues = GfxdHelperPrms.persistQueues();
    boolean persistTables = GfxdHelperPrms.persistTables();
    boolean createDiskStore = GfxdHelperPrms.createDiskStore();
    p.setProperty("persist-dd", String.valueOf(persistDD));
    if (persistDD || persistQueues || persistTables || createDiskStore) {
      p.setProperty("sys-disk-dir", getDiskDirName());
    }
    Log.getLogWriter().info("Configured connection properties: " + p);
    return p;
  }

  /*
   * Autogenerates the disk directory name for this VM using the same path as
   * the system directory.  For example,
   *
   *    /export/.../test-0328-112342/vm_3_client2_disk
   *
   * @throws HydraRuntimeException if a directory cannot be created.
   */
  private static synchronized String getDiskDirName() {
    if (diskDirName == null) {
      GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
      String path = (new File(gfd.getSysDirName())).getParent();
      // do not add pid here since dir must survive pid change
      String dir = path + File.separator + "vm_" + RemoteTestModule.getMyVmid()
                 + "_" + RemoteTestModule.getMyClientName() + "_disk";
      try {
        FileUtil.mkdir(dir);
        HostDescription hd = gfd.getHostDescription();
        try {
          RemoteTestModule.Master.recordDir(hd, gfd.getName(), dir);
        } catch (RemoteException e) {
          String s = "Unable to access master to record directory: " + dir;
          throw new HydraRuntimeException(s, e);
        }
      } 
      catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      }
      catch (Error e) {
        String s = "Unable to create directory: " + dir;
        throw new HydraRuntimeException(s);
      }
      diskDirName = dir;
    }
    return diskDirName;
  }

}
