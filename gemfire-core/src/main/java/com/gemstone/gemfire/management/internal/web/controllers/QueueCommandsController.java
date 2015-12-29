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
package com.gemstone.gemfire.management.internal.web.controllers;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The QueueCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Queue Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.QueueCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 7.5
 */
@Controller("queueController")
@RequestMapping("/v1")
@SuppressWarnings("unused")
public class QueueCommandsController extends AbstractCommandsController {

  @RequestMapping(value = "/async-event-queue", method = RequestMethod.POST)
  @ResponseBody
  public String createAsyncEventQueue(@RequestParam(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID) final String asyncEventQueueId,
                                      @RequestParam(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER) final String listener,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, required = false) final String[] listenerParametersValues,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, required = false) final String group,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, defaultValue = "100") final Integer batchSize,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, defaultValue = "false") final Boolean persistent,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, required = false) final String diskStore,
                                      @RequestParam(value = CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, defaultValue = "100") final Integer maxQueueMemory)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);

    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, asyncEventQueueId);
    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER, listener);

    if (hasValue(listenerParametersValues)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER_PARAM_AND_VALUE, StringUtils.concat(
        listenerParametersValues, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(group)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__GROUP, group);
    }

    if (hasValue(batchSize)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, String.valueOf(batchSize));
    }

    command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, String.valueOf(Boolean.TRUE.equals(persistent)));

    if (hasValue(diskStore)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, diskStore);
    }

    if (hasValue(maxQueueMemory)) {
      command.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, String.valueOf(maxQueueMemory));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/async-event-queues", method = RequestMethod.GET)
  @ResponseBody
  public String listAsyncEventQueues() {
    return processCommand(CliStrings.LIST_ASYNC_EVENT_QUEUES);
  }

}
