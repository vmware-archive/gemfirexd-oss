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
 * The DiskStoreCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Disk Store Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.DiskStoreCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 7.5
 */
@Controller("diskStoreController")
@RequestMapping("/v1")
@SuppressWarnings("unused")
public class DiskStoreCommandsController extends AbstractCommandsController {

  @RequestMapping(value = "/diskstores", method = RequestMethod.GET)
  @ResponseBody
  public String listDiskStores() {
    return processCommand(CliStrings.LIST_DISK_STORE);
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/diskstores/backup", method = RequestMethod.POST)
  @ResponseBody
  public String backupDiskStore(@RequestParam(value = CliStrings.BACKUP_DISK_STORE__DISKDIRS) final String dir,
                                @RequestParam(value = CliStrings.BACKUP_DISK_STORE__BASELINEDIR) final String baselineDir)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.BACKUP_DISK_STORE);

    command.addOption(CliStrings.BACKUP_DISK_STORE__DISKDIRS, decode(dir));

    if (hasValue(baselineDir)) {
      command.addOption(CliStrings.BACKUP_DISK_STORE__BASELINEDIR, decode(baselineDir));
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/diskstores/{name}/compact", method = RequestMethod.POST)
  @ResponseBody
  public String compactDiskStore(@PathVariable("name") final String diskStoreNameId,
                                 @RequestParam(value = CliStrings.COMPACT_DISK_STORE__GROUP) final String[] groups)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.COMPACT_DISK_STORE);

    command.addOption(CliStrings.COMPACT_DISK_STORE__NAME, diskStoreNameId);

    if (hasValue(groups)) {
      command.addOption(CliStrings.COMPACT_DISK_STORE__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/diskstores", method = RequestMethod.POST)
  @ResponseBody
  public String createDiskStore(@RequestParam(CliStrings.CREATE_DISK_STORE__NAME) final String diskStoreNameId,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE) final String[] directoryAndSizes,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION, defaultValue = "false") final Boolean allowForceCompaction,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__AUTO_COMPACT, defaultValue = "true") final Boolean autoCompact,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD, defaultValue = "50") final Integer compactionThreshold,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE, defaultValue = "1024") final Integer maxOplogSize,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__QUEUE_SIZE, defaultValue = "0") final Integer queueSize,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__TIME_INTERVAL, defaultValue = "1000") final Long timeInterval,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE, defaultValue = "32768") final Integer writeBufferSize,
                                @RequestParam(value = CliStrings.CREATE_DISK_STORE__GROUP, required = false) final String[] groups)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE);

    command.addOption(CliStrings.CREATE_DISK_STORE__NAME, diskStoreNameId);
    command.addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, StringUtils.concat(directoryAndSizes, StringUtils.COMMA_DELIMITER));
    command.addOption(CliStrings.CREATE_DISK_STORE__ALLOW_FORCE_COMPACTION, String.valueOf(Boolean.TRUE.equals(allowForceCompaction)));
    command.addOption(CliStrings.CREATE_DISK_STORE__AUTO_COMPACT, String.valueOf(Boolean.TRUE.equals(autoCompact)));
    command.addOption(CliStrings.CREATE_DISK_STORE__COMPACTION_THRESHOLD, String.valueOf(compactionThreshold));
    command.addOption(CliStrings.CREATE_DISK_STORE__MAX_OPLOG_SIZE, String.valueOf(maxOplogSize));
    command.addOption(CliStrings.CREATE_DISK_STORE__QUEUE_SIZE, String.valueOf(queueSize));
    command.addOption(CliStrings.CREATE_DISK_STORE__TIME_INTERVAL, String.valueOf(timeInterval));
    command.addOption(CliStrings.CREATE_DISK_STORE__WRITE_BUFFER_SIZE, String.valueOf(writeBufferSize));

    if (hasValue(groups)) {
      command.addOption(CliStrings.CREATE_DISK_STORE__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/diskstores/{name}", method = RequestMethod.GET)
  @ResponseBody
  public String describeDiskStore(@PathVariable("name") final String diskStoreNameId,
                                  @RequestParam(CliStrings.DESCRIBE_DISK_STORE__MEMBER) final String memberNameId)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESCRIBE_DISK_STORE);
    command.addOption(CliStrings.DESCRIBE_DISK_STORE__MEMBER, memberNameId);
    command.addOption(CliStrings.DESCRIBE_DISK_STORE__NAME, decode(diskStoreNameId, DEFAULT_ENCODING));
    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/diskstores/{name}", method = RequestMethod.DELETE)
  @ResponseBody
  public String destroyDiskStore(@PathVariable("name") final String diskStoreName,
                                 @RequestParam(value = CliStrings.DESTROY_DISK_STORE__GROUP) final String[] groupNames)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESTROY_DISK_STORE);

    command.addOption(CliStrings.DESTROY_DISK_STORE__NAME, diskStoreName);

    if (hasValue(groupNames)) {
      command.addOption(CliStrings.DESTROY_DISK_STORE__GROUP, StringUtils.concat(groupNames, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/diskstores/{id}/revoke", method = RequestMethod.POST)
  @ResponseBody
  public String revokeMissingDiskStore(@PathVariable("id") final String diskStoreId) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.REVOKE_MISSING_DISK_STORE);
    command.addOption(CliStrings.REVOKE_MISSING_DISK_STORE__ID, diskStoreId);
    return processCommand(command.toString());
  }

  @RequestMapping(value = "/diskstores/missing", method = RequestMethod.GET)
  @ResponseBody
  public String showMissingDiskStores() {
    return processCommand(CliStrings.SHOW_MISSING_DISK_STORE);
  }

}
