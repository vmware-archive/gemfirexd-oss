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
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The MiscellaneousCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Miscellaneous Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.MiscellaneousCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 7.5
 */
@Controller("miscellaneousController")
@RequestMapping("/v1")
@SuppressWarnings("unused")
public class MiscellaneousCommandsController extends AbstractCommandsController {

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/logs/export", method = RequestMethod.GET)
  @ResponseBody
  public String exportLogs(@RequestParam(CliStrings.EXPORT_LOGS__DIR) final String directory,
                           @RequestParam(value = CliStrings.EXPORT_LOGS__GROUP, required = false) final String[] groups,
                           @RequestParam(value = CliStrings.EXPORT_LOGS__MEMBER, required = false) final String memberNameId,
                           @RequestParam(value = CliStrings.EXPORT_LOGS__LOGLEVEL, required = false) final String logLevel,
                           @RequestParam(value = CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL, defaultValue = "false") final Boolean onlyLogLevel,
                           @RequestParam(value = CliStrings.EXPORT_LOGS__MERGELOG, defaultValue = "false") final Boolean mergeLog,
                           @RequestParam(value = CliStrings.EXPORT_LOGS__STARTTIME, required = false) final String startTime,
                           @RequestParam(value = CliStrings.EXPORT_LOGS__ENDTIME, required = false) final String endTime)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_LOGS);

    command.addOption(CliStrings.EXPORT_LOGS__DIR, decode(directory));

    if (hasValue(groups)) {
      command.addOption(CliStrings.EXPORT_LOGS__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.EXPORT_LOGS__MEMBER, memberNameId);
    }

    if (hasValue(logLevel)) {
      command.addOption(CliStrings.EXPORT_LOGS__LOGLEVEL, logLevel);
    }

    command.addOption(CliStrings.EXPORT_LOGS__UPTO_LOGLEVEL, String.valueOf(Boolean.TRUE.equals(onlyLogLevel)));
    command.addOption(CliStrings.EXPORT_LOGS__MERGELOG, String.valueOf(Boolean.TRUE.equals(mergeLog)));

    if (hasValue(startTime)) {
      command.addOption(CliStrings.EXPORT_LOGS__STARTTIME, startTime);
    }

    if (hasValue(endTime)) {
      command.addOption(CliStrings.EXPORT_LOGS__ENDTIME, endTime);
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/stacktraces/export", method = RequestMethod.GET)
  @ResponseBody
  public String exportStackTraces(@RequestParam(value = CliStrings.EXPORT_STACKTRACE__FILE) final String file,
                                  @RequestParam(value = CliStrings.EXPORT_STACKTRACE__GROUP, required = false) final String groupName,
                                  @RequestParam(value = CliStrings.EXPORT_STACKTRACE__MEMBER, required = false) final String memberNameId) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_STACKTRACE);

    command.addOption(CliStrings.EXPORT_STACKTRACE__FILE, decode(file));

    if (hasValue(groupName)) {
      command.addOption(CliStrings.EXPORT_STACKTRACE__GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.EXPORT_STACKTRACE__MEMBER, memberNameId);
    }

    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(value = "/gc", method = RequestMethod.POST)
  @ResponseBody
  public String gc(@RequestParam(value = CliStrings.GC__GROUP, required = false) final String[] groups) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.GC);

    if (hasValue(groups)) {
      command.addOption(CliStrings.GC__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(value = "/members/{member}/gc", method = RequestMethod.POST)
  @ResponseBody
  public String gc(@PathVariable("member") final String memberNameId) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.GC);
    command.addOption(CliStrings.GC__MEMBER, memberNameId);
    return processCommand(command.toString());
  }

  // TODO determine if Async functionality is required
  @RequestMapping(value = "/deadlocks", method = RequestMethod.GET)
  @ResponseBody
  public String showDeadLock(@RequestParam(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE) final String dependenciesFile) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_DEADLOCK);
    command.addOption(CliStrings.SHOW_DEADLOCK__DEPENDENCIES__FILE, decode(dependenciesFile));
    return processCommand(command.toString());
  }

  @RequestMapping(value = "/members/{member}/log", method = RequestMethod.GET)
  @ResponseBody
  public String showLog(@PathVariable("member") final String memberNameId,
                        @RequestParam(value = CliStrings.SHOW_LOG_LINE_NUM, defaultValue = "0") final Integer lines)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_LOG);

    command.addOption(CliStrings.SHOW_LOG_MEMBER, memberNameId);
    command.addOption(CliStrings.SHOW_LOG_LINE_NUM, String.valueOf(lines));

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/metrics", method = RequestMethod.GET)
  @ResponseBody
  public String showMetrics(@RequestParam(value = CliStrings.SHOW_METRICS__MEMBER, required = false) final String memberNameId,
                            @RequestParam(value = CliStrings.SHOW_METRICS__REGION, required = false) final String regionNamePath,
                            @RequestParam(value = CliStrings.SHOW_METRICS__FILE, required = false) final String file,
                            @RequestParam(value = CliStrings.SHOW_METRICS__CACHESERVER__PORT, required = false) final String cacheServerPort,
                            @RequestParam(value = CliStrings.SHOW_METRICS__CATEGORY, required = false) final String[] categories)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHOW_METRICS);

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.SHOW_METRICS__MEMBER, memberNameId);
    }

    if (hasValue(regionNamePath)) {
      command.addOption(CliStrings.SHOW_METRICS__REGION, regionNamePath);
    }

    if (hasValue(file)) {
      command.addOption(CliStrings.SHOW_METRICS__FILE, file);
    }

    if (hasValue(cacheServerPort)) {
      command.addOption(CliStrings.SHOW_METRICS__CACHESERVER__PORT, cacheServerPort);
    }

    if (hasValue(categories)) {
      command.addOption(CliStrings.SHOW_METRICS__CATEGORY, StringUtils.concat(categories, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/shutdown", method = RequestMethod.POST)
  @ResponseBody
  public String shutdown(@RequestParam(value = CliStrings.SHUTDOWN__TIMEOUT, defaultValue = "-1") final Integer timeout) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.SHUTDOWN);
    command.addOption(CliStrings.SHUTDOWN__TIMEOUT, String.valueOf(timeout));
    return processCommand(command.toString());
  }

}
