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
package com.gemstone.gemfire.management.internal.cli.commands;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.management.cli.CliMetaData;
import com.gemstone.gemfire.management.cli.ConverterHint;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.domain.AsyncEventQueueDetails;
import com.gemstone.gemfire.management.internal.cli.functions.CliFunctionResult;
import com.gemstone.gemfire.management.internal.cli.functions.CreateAsyncEventQueueFunction;
import com.gemstone.gemfire.management.internal.cli.functions.ListAsyncEventQueuesFunction;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResultException;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 * The QueueCommands class encapsulates all GemFire Queue commands in Gfsh.
 * </p>
 * @author David Hoots
 * 
 * @since 7.5
 */
public class QueueCommands implements CommandMarker {
  private Gfsh getGfsh() {
    return Gfsh.getCurrentInstance();
  }

  @CliCommand(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE, help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__HELP)
  public Result createAsyncEventQueue(
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, 
                 mandatory = true,
                 help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID__HELP)
      String id,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, 
                 unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, 
                 optionContext = ConverterHint.MEMBERGROUP,
                 help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP__HELP)
      String group,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, 
                 unspecifiedDefaultValue = "100", 
                 help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE__HELP)
      int batchSize,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, 
                 unspecifiedDefaultValue = "false",
                 specifiedDefaultValue = "true",
                 help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT__HELP)
      boolean persistent,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, 
                 unspecifiedDefaultValue = CliMetaData.ANNOTATION_NULL_VALUE, 
                 help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE__HELP)
      String diskStore,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY,
                 unspecifiedDefaultValue = "100", 
                 help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY__HELP)
      int maxQueueMemory,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
                 mandatory = true,
                 help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER__HELP)
      String listener,
      @CliOption(key = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, 
                 help = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE__HELP,
                 optionContext=ConverterHint.STRING_DISABLER)
      @CliMetaData (valueSeparator = ",")
      String[] listenerParamsAndValues) {

    Properties listenerProperties = new Properties();

    try {
      if (listenerParamsAndValues != null) {
        for (int i = 0; i < listenerParamsAndValues.length; i++) {
          final int hashPosition = listenerParamsAndValues[i].indexOf('#');
          if (hashPosition == -1) {
            listenerProperties.put(listenerParamsAndValues[i], "");
          } else {
            listenerProperties.put(listenerParamsAndValues[i].substring(0, hashPosition), listenerParamsAndValues[i]
                .substring(hashPosition + 1));
          }
        }
      }

      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(group, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(new CreateAsyncEventQueueFunction(), new Object[] { id, batchSize,
          persistent, diskStore, maxQueueMemory, listener, listenerProperties }, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName() + ": "
              + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else if (result.isSuccessful()) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", result.getMessage());
          accumulatedData = true;
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult("Unable to create async event queue(s).");
      }

      return ResultBuilder.buildResult(tabularData);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(
          CliStrings.CREATE_ASYNC_EVENT_QUEUE__ERROR_WHILE_CREATING_REASON_0, new Object[] { th.getMessage() }));
    }
  }

  @CliCommand(value = CliStrings.LIST_ASYNC_EVENT_QUEUES, help = CliStrings.LIST_ASYNC_EVENT_QUEUES__HELP)
  public Result listAsyncEventQueues() {
    try {
      TabularResultData tabularData = ResultBuilder.createTabularResultData();
      boolean accumulatedData = false;

      Set<DistributedMember> targetMembers;
      try {
        targetMembers = CliUtil.findAllMatchingMembers(null, null);
      } catch (CommandResultException crex) {
        return crex.getResult();
      }

      ResultCollector<?, ?> rc = CliUtil.executeFunction(new ListAsyncEventQueuesFunction(), new Object[] {}, targetMembers);
      List<CliFunctionResult> results = CliFunctionResult.cleanResults((List<?>) rc.getResult());

      for (CliFunctionResult result : results) {
        if (result.getThrowable() != null) {
          tabularData.accumulate("Member", result.getMemberIdOrName());
          tabularData.accumulate("Result", "ERROR: " + result.getThrowable().getClass().getName() + ": "
              + result.getThrowable().getMessage());
          accumulatedData = true;
          tabularData.setStatus(Status.ERROR);
        } else {
          AsyncEventQueueDetails[] details = (AsyncEventQueueDetails[]) result.getSerializables();
          for (int i = 0; i < details.length; i++) {
            tabularData.accumulate("Member", result.getMemberIdOrName());
            tabularData.accumulate("ID", details[i].getId());
            tabularData.accumulate("Batch Size", details[i].getBatchSize());
            tabularData.accumulate("Persistent", details[i].isPersistent());
            tabularData.accumulate("Disk Store", details[i].getDiskStoreName());
            tabularData.accumulate("Max Memory", details[i].getMaxQueueMemory());

            Properties listenerProperties = details[i].getListenerProperties();
            if (listenerProperties == null || listenerProperties.size() == 0) {
              tabularData.accumulate("Listener", details[i].getListener());
            } else {
              StringBuilder propsStringBuilder = new StringBuilder();
              propsStringBuilder.append('(');
              boolean firstProperty = true;
              for (Map.Entry<Object, Object> property : listenerProperties.entrySet()) {
                if (!firstProperty) {
                  propsStringBuilder.append(',');
                } else {
                  firstProperty = false;
                }
                propsStringBuilder.append(property.getKey()).append('=').append(property.getValue());
              }
              propsStringBuilder.append(')');
              
              tabularData.accumulate("Listener", details[i].getListener() + propsStringBuilder.toString());
            }
            accumulatedData = true;
          }
        }
      }

      if (!accumulatedData) {
        return ResultBuilder.createInfoResult(CliStrings.LIST_ASYNC_EVENT_QUEUES__NO_QUEUES_FOUND_MESSAGE);
      }

      return ResultBuilder.buildResult(tabularData);
    } catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable th) {
      SystemFailure.checkFailure();
      return ResultBuilder.createGemFireErrorResult(CliStrings.format(
          CliStrings.LIST_ASYNC_EVENT_QUEUES__ERROR_WHILE_LISTING_REASON_0, new Object[] { th.getMessage() }));
    }
  }

  @CliAvailabilityIndicator({ CliStrings.CREATE_ASYNC_EVENT_QUEUE, CliStrings.LIST_ASYNC_EVENT_QUEUES })
  public boolean queueCommandsAvailable() {
    boolean isAvailable = true;
    if (CliUtil.isGfshVM()) {
      isAvailable = getGfsh() != null && getGfsh().isConnectedAndReady();
    }
    return isAvailable;
  }
}
