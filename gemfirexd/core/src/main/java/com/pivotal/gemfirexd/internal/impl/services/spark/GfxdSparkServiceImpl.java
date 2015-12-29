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
package com.pivotal.gemfirexd.internal.impl.services.spark;

import java.util.Properties;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

public class GfxdSparkServiceImpl implements ModuleControl {

  private static final String GFXD_SPARK_SERVICE_IMPL =
      "com.pivotal.gemfirexd.internal.impl.services.spark.GfxdSparkContainerImpl$";

  private final ModuleControl instance;

  public GfxdSparkServiceImpl() {
    ModuleControl local = null;
    try {
      Class<?> clazz = Class.forName(GFXD_SPARK_SERVICE_IMPL);
      local = (ModuleControl)clazz.getField("MODULE$").get(null);
    } catch (Exception e) {
      // ignore
      // e.printStackTrace();
    }
    this.instance = local;
  }

  @Override
  public void boot(boolean create, Properties properties)
      throws StandardException {
    final ModuleControl instance = this.instance;
    if (instance != null) {
      // TODO:Asif : hack. figure out the problem
      properties.put("GfxdSerializable.GFXD_SPARK_PROFILE", ""
          + GfxdSerializable.GFXD_SPARK_PROFILE);
      properties.put("GfxdSerializable.GFXD_SPARK_TASK_SUBMIT", ""
          + GfxdSerializable.GFXD_SPARK_TASK_SUBMIT);
      properties.put("GfxdSerializable.GFXD_SPARK_TASK_RESULT", ""
          + GfxdSerializable.GFXD_SPARK_TASK_RESULT);
      instance.boot(create, properties);
    }
  }

  @Override
  public void stop() {
    final ModuleControl instance = this.instance;
    if (instance != null) {
      try {
        instance.stop();
      } catch (Exception e) {
        // ignore exceptions during stop for shutdown to continue
        SanityManager.DEBUG_PRINT("error:"
            + GfxdConstants.TRACE_FABRIC_SERVICE_BOOT,
            "GfxdSparkServiceImpl exception in stop", e);
      }
    }
  }
}
