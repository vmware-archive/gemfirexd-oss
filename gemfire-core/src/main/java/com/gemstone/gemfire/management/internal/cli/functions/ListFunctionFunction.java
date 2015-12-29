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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalEntity;

public class ListFunctionFunction implements Function, InternalEntity {
  public static final String ID = ListFunctionFunction.class.getName();

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    // Declared here so that it's available when returning a Throwable
    String memberId = "";
    LogWriter logger = null;

    try {
      final Object[] args = (Object[]) context.getArguments();
      final String stringPattern = (String) args[0];

      Cache cache = CacheFactory.getAnyInstance();
      logger = cache.getLogger();
      DistributedMember member = cache.getDistributedSystem().getDistributedMember();

      memberId = member.getId();
      // If they set a name use it instead
      if (!member.getName().equals("")) {
        memberId = member.getName();
      }

      final Map<String, Function> functions = FunctionService.getRegisteredFunctions();
      CliFunctionResult result;
      if (stringPattern == null || stringPattern.isEmpty()) {
        result = new CliFunctionResult(memberId, functions.keySet().toArray(new String[0]));
      } else {
        Pattern pattern = Pattern.compile(stringPattern);
        List<String> resultList = new LinkedList<String>();
        for (String functionId : functions.keySet()) {
          Matcher matcher = pattern.matcher(functionId);
          if (matcher.matches()) {
            resultList.add(functionId);
          }
        }
        result = new CliFunctionResult(memberId, resultList.toArray(new String[0]));
      }
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
        logger.error("Could not list functions", th);
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
