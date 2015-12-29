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
/**
 * 
 */
package vsphere.vijava;

import hydra.ClientVmInfo;

public final class VMotionUtil {

  /**
   * Trigger vMotion if appropriate STOP_START flags are set. 
   * @param vmId
   *          The id of hydra-vm being restarted.
   */
  public static void doVMotion(ClientVmInfo targetVm) {
    Boolean moveVM = (Boolean)VMotionBB.getBB().getSharedMap()
        .get(VIJavaUtil.VMOTION_DURING_STOP_START);

    if (moveVM == null || !moveVM) {
      Boolean moveLocator = (Boolean)VMotionBB.getBB().getSharedMap()
          .get(VIJavaUtil.VMOTION_DURING_LOCATOR_STOP_START);
      if (moveLocator != null && moveLocator) {
        if (targetVm.getClientName().indexOf("locator") == -1) {
          // It's not a locator vm.
          return;
        }
      } else {
        // We neither want to move a normal vm nor a locator vm.
        return;
      }
    }
    VIJavaUtil.doVMotion(targetVm);
  }
}
