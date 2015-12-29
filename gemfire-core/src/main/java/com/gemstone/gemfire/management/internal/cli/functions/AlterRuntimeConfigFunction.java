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

import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

/****
 * 
 * @author Sourabh Bansod
 *
 */
public class AlterRuntimeConfigFunction extends FunctionAdapter implements
InternalEntity {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    String memberId = "";

    try {
      Object arg = context.getArguments();
      GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();
      DistributionConfig config = cache.getSystem().getConfig();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();

      Map<String, String> runtimeAttributes = (Map<String, String>)arg;
      Set<String> attributeNames = runtimeAttributes.keySet();

      for (String attributeName : attributeNames) {
        String attValue = runtimeAttributes.get(attributeName);
        config.setAttribute(attributeName, attValue, ConfigSource.runtime());
      }
      CliFunctionResult cliFuncResult = new CliFunctionResult(memberId, true, null);
      context.getResultSender().lastResult(cliFuncResult);
      
    } catch (CacheClosedException cce) {
      CliFunctionResult result = new CliFunctionResult(memberId, false, null);
      context.getResultSender().lastResult(result);
      
    } catch (Exception e){
      CliFunctionResult cliFuncResult = new CliFunctionResult(memberId, e, null);
      context.getResultSender().lastResult(cliFuncResult);    
    }
  }

  @Override
  public String getId() {
    return AlterRuntimeConfigFunction.class.getName();
  }
}
