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
 * The ConfigCommandsController class implements GemFire Management REST API web service endpoints for the Gfsh
 * Config Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.ConfigCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 7.5
 */
@Controller("configController")
@RequestMapping("/v1")
@SuppressWarnings("unused")
public class ConfigCommandsController extends AbstractCommandsController {

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/config", method = RequestMethod.POST)
  @ResponseBody
  public String alterRuntime(@RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__GROUP, required = false) final String group,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__MEMBER, required = false) final String memberNameId,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, required = false) final String archiveDiskSpaceLimit,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, required = false) final String archiveFileSizeLimit,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, required = false) final String logDiskSpaceLimit,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, required = false) final String logFileSizeLimit,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, required = false) final String logLevel,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, required = false) final String statisticsArchiveFile,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, required = false) final String statisticsSampleRate,
                             @RequestParam(value = CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, required = false) final Boolean enableStatistics)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__MEMBER, memberNameId);
    }

    if (hasValue(group)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__GROUP, group);
    }

    if (hasValue(archiveDiskSpaceLimit)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, archiveDiskSpaceLimit);
    }

    if (hasValue(archiveFileSizeLimit)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, archiveFileSizeLimit);
    }

    if (hasValue(logDiskSpaceLimit)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, logDiskSpaceLimit);
    }

    if (hasValue(logFileSizeLimit)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, logFileSizeLimit);
    }

    if (hasValue(logLevel)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, logLevel);
    }

    if (hasValue(statisticsArchiveFile)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, statisticsArchiveFile);
    }

    if (hasValue(statisticsSampleRate)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, statisticsSampleRate);
    }

    if (hasValue(enableStatistics)) {
      command.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, String.valueOf(enableStatistics));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/members/{member}/config", method = RequestMethod.GET)
  @ResponseBody
  public String describeConfig(@PathVariable("member") final String memberNameId,
                               @RequestParam(value = CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS, defaultValue = "true") final Boolean hideDefaults)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESCRIBE_CONFIG);

    command.addOption(CliStrings.DESCRIBE_CONFIG__MEMBER, memberNameId);
    command.addOption(CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS, String.valueOf(hideDefaults));

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/config", method = RequestMethod.GET)
  @ResponseBody
  public String exportConfig(@RequestParam(value = CliStrings.EXPORT_CONFIG__GROUP, required = false) final String[] groups,
                             @RequestParam(value = CliStrings.EXPORT_CONFIG__MEMBER, required = false) final String[] members,
                             @RequestParam(value = CliStrings.EXPORT_CONFIG__DIR, required = false) final String directory)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_CONFIG);

    if (hasValue(groups)) {
      command.addOption(CliStrings.EXPORT_CONFIG__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(members)) {
      command.addOption(CliStrings.EXPORT_CONFIG__MEMBER, StringUtils.concat(members, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(directory)) {
      command.addOption(CliStrings.EXPORT_CONFIG__DIR, decode(directory));
    }

    return processCommand(command.toString());
  }

}
