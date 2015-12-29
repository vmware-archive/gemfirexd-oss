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
package com.gemstone.gemfire.management.internal.cli.remote;

import java.lang.reflect.Method;

import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliAroundInterceptor;
import com.gemstone.gemfire.management.internal.cli.GfshParseResult;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.result.FileResult;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.util.spring.Assert;
import com.gemstone.gemfire.management.internal.cli.util.spring.ReflectionUtils;

/**
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
// Doesn't have to be org.springframework.roo.shell.ExecutionStrategy
public class RemoteExecutionStrategy {
  private LogWrapper logWrapper = LogWrapper.getInstance();
  public Object execute(ParseResult parseResult) throws RuntimeException {
    Result result = null;
    try {
      
      Assert.notNull(parseResult, "Parse result required");
      if (!GfshParseResult.class.isInstance(parseResult)) {
        //Remote command means implemented for Gfsh and ParseResult should be GfshParseResult.
        //TODO: should this message be more specific?
        throw new IllegalArgumentException("Command Configuration/Definition error.");
      }
      
      GfshParseResult gfshParseResult = (GfshParseResult) parseResult;
      
      Method method = gfshParseResult.getMethod();
      
      if (!isShellOnly(method)) {
        Boolean fromShell = CommandExecutionContext.isShellRequest();
        boolean sentFromShell = fromShell != null && fromShell.booleanValue();
        String interceptorClass = getInterceptor(gfshParseResult.getMethod());
        CliAroundInterceptor interceptor = null;

        // 1. Pre Execution
        if (!sentFromShell && !CliMetaData.ANNOTATION_NULL_VALUE.equals(interceptorClass)) {
          try {
            interceptor = (CliAroundInterceptor) ClassPathLoader.getLatest().forName(interceptorClass).newInstance();
          } catch (InstantiationException e) {
            logWrapper.info(e.getMessage());
          } catch (IllegalAccessException e) {
            logWrapper.info(e.getMessage());
          } catch (ClassNotFoundException e) {
            logWrapper.info(e.getMessage());
          }
          if (interceptor != null) {
            Result preExecResult = interceptor.preExecution(gfshParseResult);
            if (Status.ERROR.equals(preExecResult.getStatus())) {
              return preExecResult;
            } else if (preExecResult instanceof FileResult) {            
              FileResult fileResult = (FileResult) preExecResult;
              byte[][]fileData = fileResult.toBytes();
              CommandExecutionContext.setBytesFromShell(fileData);
            }
          } else {
            return ResultBuilder.createBadConfigurationErrorResult("Interceptor Configuration Error");
          }
        }

        //2. Actual Execution
        result = (Result) ReflectionUtils.invokeMethod(gfshParseResult.getMethod(), gfshParseResult.getInstance(), gfshParseResult.getArguments());

        if (result != null && Status.ERROR.equals(result.getStatus())) {
          logWrapper.info("Error occurred while executing \""+gfshParseResult.getUserInput()+"\".");
        }
        
        //3. Post Execution
        if (interceptor != null) {
          Result postExecResult = interceptor.postExecution(gfshParseResult, result);
          if (postExecResult != null) {
            if (Status.ERROR.equals(postExecResult.getStatus())) {
              logWrapper.warning(postExecResult.toString(), null);
            } else if (logWrapper.fineEnabled()) {
              logWrapper.fine(String.valueOf(postExecResult));
            }
            result = postExecResult;
          }
          CommandExecutionContext.setBytesFromShell(null); // for remote commands with bytes
        }
      } else {
        throw new IllegalArgumentException(
            "Only Remote command can be executed through "
                + ManagementService.class.getSimpleName()
                + ".processCommand() or ManagementMBean's processCommand " 
                + "operation. Please refer documentation for the list of " 
                + "commands.");
      }
    } catch(RuntimeException e) {
      throw e;
    }
    
    return result;
  }
  
  private boolean isShellOnly(Method method) {
    CliMetaData cliMetadata = method.getAnnotation(CliMetaData.class);
    return cliMetadata != null && cliMetadata.shellOnly();
  }

  private String getInterceptor(Method method) {
    CliMetaData cliMetadata = method.getAnnotation(CliMetaData.class);
    return cliMetadata != null ? cliMetadata.interceptor() : CliMetaData.ANNOTATION_NULL_VALUE;
  }

  public void terminate() {
  }
}
