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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Map;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;

public class ExportConfigFunction implements Function, InternalEntity {
  public static final String ID = ExportConfigFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";
    LogWriter logger = null;

    try {
      Cache cache = CacheFactory.getAnyInstance();
      logger = cache.getLogger();
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      // Generate the cache XML
      StringWriter xmlWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(xmlWriter);
      CacheXmlGenerator.generate(cache, printWriter, false, false, false);
      printWriter.close();
      
      // Generate the properties file
      DistributionConfigImpl  config = (DistributionConfigImpl) ((InternalDistributedSystem) cache.getDistributedSystem()).getConfig();
      StringBuffer propStringBuf = new StringBuffer();
      String lineSeparator = System.getProperty("line.separator");
      for (Map.Entry entry : config.getConfigPropsFromSource(ConfigSource.runtime()).entrySet()) {
        if (entry.getValue() != null && !entry.getValue().equals("")) {
          propStringBuf.append(entry.getKey()).append("=").append(entry.getValue()).append(lineSeparator);
        }
      }
      for (Map.Entry entry : config.getConfigPropsFromSource(ConfigSource.api()).entrySet()) {
        if (entry.getValue() != null && !entry.getValue().equals("")) {
          propStringBuf.append(entry.getKey()).append("=").append(entry.getValue()).append(lineSeparator);
        }
      }
      for (Map.Entry entry : config.getConfigPropsDefinedUsingFiles().entrySet()) {
        if (entry.getValue() != null && !entry.getValue().equals("")) {
          propStringBuf.append(entry.getKey()).append("=").append(entry.getValue()).append(lineSeparator);
        }
      }
      // fix for bug 46653
      for (Map.Entry entry : config.getConfigPropsFromSource(ConfigSource.launcher()).entrySet()) {
        if (entry.getValue() != null && !entry.getValue().equals("")) {
          propStringBuf.append(entry.getKey()).append("=").append(entry.getValue()).append(lineSeparator);
        }
      }
      
      CliFunctionResult result = new CliFunctionResult(memberId, new String[] { xmlWriter.toString(), propStringBuf.toString() });

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
        logger.error("Could not export config", th);
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