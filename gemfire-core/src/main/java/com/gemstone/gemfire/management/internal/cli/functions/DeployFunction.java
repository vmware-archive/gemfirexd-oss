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
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.JarClassLoader;
import com.gemstone.gemfire.internal.JarDeployer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

public class DeployFunction implements Function, InternalEntity {
  public static final String ID = DeployFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";
    LogWriter logger = null;

    try {
      final Object[] args = (Object[]) context.getArguments();
      final String[] jarFilenames = (String[]) args[0];
      final byte[][] jarBytes = (byte[][]) args[1];
      Cache cache = CacheFactory.getAnyInstance();

      logger = cache.getLogger();
      final JarDeployer jarDeployer = new JarDeployer(logger, ((GemFireCacheImpl) cache).getDistributedSystem().getConfig().getDeployWorkingDir());

      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      List<String> deployedList = new ArrayList<String>();
      JarClassLoader[] jarClassLoaders = jarDeployer.deploy(jarFilenames, jarBytes);
      for (int i = 0; i < jarFilenames.length; i++) {
        deployedList.add(jarFilenames[i]);
        if (jarClassLoaders[i] != null) {
          deployedList.add(jarClassLoaders[i].getFileCanonicalPath());
        } else {
          deployedList.add("Already deployed");
        }
      }
      
      CliFunctionResult result = new CliFunctionResult(memberId, deployedList.toArray(new String[0]));
      context.getResultSender().lastResult(result);
      
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);
      
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
      
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      if (logger != null) {
        logger.error("Could not deploy JAR file", th);
      }
      
      CliFunctionResult result = new CliFunctionResult(memberId, th, null);
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
