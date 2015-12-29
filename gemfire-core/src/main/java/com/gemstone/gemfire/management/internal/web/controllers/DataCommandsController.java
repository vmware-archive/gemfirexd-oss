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

import java.util.concurrent.Callable;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The DataCommandsController class implements GemFire Management REST API web service endpoints for the
 * Gfsh Data Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.DataCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 7.5
 */
@Controller("dataController")
@RequestMapping("/v1")
@SuppressWarnings("unused")
public class DataCommandsController extends AbstractCommandsController {

  @RequestMapping(value = "/regions/{region}/data", method = RequestMethod.GET)
  @ResponseBody
  public String get(@PathVariable("region") final String regionNamePath,
                    @RequestParam(CliStrings.GET__KEY) final String key,
                    @RequestParam(value = CliStrings.GET__KEYCLASS, required = false) final String keyClassName,
                    @RequestParam(value = CliStrings.GET__VALUEKLASS, required = false) final String valueClassName)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.GET);

    command.addOption(CliStrings.GET__REGIONNAME, regionNamePath);
    command.addOption(CliStrings.GET__KEY, key);

    if (hasValue(keyClassName)) {
      command.addOption(CliStrings.GET__KEYCLASS, keyClassName);
    }

    if (hasValue(valueClassName)) {
      command.addOption(CliStrings.GET__VALUEKLASS, valueClassName);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/regions/{region}/data", method = RequestMethod.PUT)
  @ResponseBody
  public String put(@PathVariable("region") final String regionNamePath,
                    @RequestParam(CliStrings.PUT__KEY) final String key,
                    @RequestParam(value = CliStrings.PUT__KEYCLASS, required = false) final String keyClassName,
                    @RequestParam(CliStrings.PUT__VALUE) final String value,
                    @RequestParam(value = CliStrings.PUT__VALUEKLASS, required = false) final String valueClassName,
                    @RequestParam(value = CliStrings.PUT__PUTIFABSENT, defaultValue = "false") final Boolean putIfAbsent)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.PUT);

    command.addOption(CliStrings.PUT__REGIONNAME, regionNamePath);
    command.addOption(CliStrings.PUT__KEY, key);
    command.addOption(CliStrings.PUT__VALUE, decode(value));

    if (hasValue(keyClassName)) {
      command.addOption(CliStrings.PUT__KEYCLASS, keyClassName);
    }

    if (hasValue(valueClassName)) {
      command.addOption(CliStrings.PUT__VALUEKLASS, valueClassName);
    }

    command.addOption(CliStrings.PUT__PUTIFABSENT, String.valueOf(putIfAbsent));

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/regions/{region}/data", method = RequestMethod.DELETE)
  @ResponseBody
  public String remove(@PathVariable("region") final String regionNamePath,
                       @RequestParam(value = CliStrings.REMOVE__ALL, defaultValue = "false") final Boolean allKeys,
                       @RequestParam(value = CliStrings.REMOVE__KEY, required = false) final String key,
                       @RequestParam(value = CliStrings.REMOVE__KEYCLASS, required = false) final String keyClassName) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.REMOVE);

    command.addOption(CliStrings.REMOVE__REGION, regionNamePath);
    command.addOption(CliStrings.REMOVE__ALL, String.valueOf(allKeys));

    if (hasValue(key)) {
      command.addOption(CliStrings.REMOVE__KEY, key);
    }

    if (hasValue(keyClassName)) {
      command.addOption(CliStrings.REMOVE__KEYCLASS, keyClassName);
    }

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/members/{member}/regions/{region}/data", method = RequestMethod.GET)
  @ResponseBody
  public String exportData(@PathVariable("member") final String memberNameId,
                           @PathVariable("region") final String regionNamePath,
                           @RequestParam(CliStrings.EXPORT_DATA__FILE) final String file)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.EXPORT_DATA);

    command.addOption(CliStrings.EXPORT_DATA__MEMBER, memberNameId);
    command.addOption(CliStrings.EXPORT_DATA__REGION, regionNamePath);
    command.addOption(CliStrings.EXPORT_DATA__FILE, decode(file));

    return processCommand(command.toString());
  }

  // TODO add Async functionality
  @RequestMapping(value = "/members/{member}/regions/{region}/data", method = RequestMethod.POST)
  @ResponseBody
  public String importData(@PathVariable("member") final String memberNameId,
                           @PathVariable("region") final String regionNamePath,
                           @RequestParam(CliStrings.IMPORT_DATA) final String file)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.IMPORT_DATA);

    command.addOption(CliStrings.IMPORT_DATA__MEMBER, memberNameId);
    command.addOption(CliStrings.IMPORT_DATA__REGION, regionNamePath);
    command.addOption(CliStrings.IMPORT_DATA__FILE, decode(file));

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/regions/{region}/data/location", method = RequestMethod.GET)
  @ResponseBody
  public String locateEntry(@PathVariable("region") final String regionNamePath,
                            @RequestParam(CliStrings.LOCATE_ENTRY__KEY) final String key,
                            @RequestParam(value = CliStrings.LOCATE_ENTRY__KEYCLASS, required = false) final String keyClassName,
                            @RequestParam(value = CliStrings.LOCATE_ENTRY__VALUEKLASS, required = false) final String valueClassName,
                            @RequestParam(value = CliStrings.LOCATE_ENTRY__RECURSIVE, defaultValue = "false") final Boolean recursive)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.LOCATE_ENTRY);

    command.addOption(CliStrings.LOCATE_ENTRY__REGIONNAME, regionNamePath);
    command.addOption(CliStrings.LOCATE_ENTRY__KEY, key);

    if (hasValue(keyClassName)) {
      command.addOption(CliStrings.LOCATE_ENTRY__KEYCLASS, keyClassName);
    }

    if (hasValue(valueClassName)) {
      command.addOption(CliStrings.LOCATE_ENTRY__VALUEKLASS, valueClassName);
    }

    command.addOption(CliStrings.LOCATE_ENTRY__RECURSIVE, String.valueOf(recursive));

    return processCommand(command.toString());
  }

  // TODO determine whether Async functionality is required
  @RequestMapping(value = "/regions/data/query", method = RequestMethod.GET)
  @ResponseBody
  public String query(@RequestParam(CliStrings.QUERY__QUERY) final String oql,
                      @RequestParam(value = CliStrings.QUERY__STEPNAME, required = false) final String stepName)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.QUERY);

    command.addOption(CliStrings.QUERY__QUERY, decode(oql));
    command.addOption(CliStrings.QUERY__INTERACTIVE, Boolean.FALSE.toString());

    if (hasValue(stepName)) {
      command.addOption(CliStrings.QUERY__STEPNAME, stepName);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/regions/data/rebalance", method = RequestMethod.POST)
  public Callable<ResponseEntity<String>> rebalance(@RequestParam(value = CliStrings.REBALANCE__INCLUDEREGION, required = false) final String[] includedRegions,
                                                    @RequestParam(value = CliStrings.REBALANCE__EXCLUDEREGION, required = false) final String[] excludedRegions,
                                                    @RequestParam(value = CliStrings.REBALANCE__SIMULATE, defaultValue = "false") final Boolean simulate,
                                                    @RequestParam(value = CliStrings.REBALANCE__TIMEOUT, defaultValue = "-1") final Long timeout)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.REBALANCE);

    if (hasValue(includedRegions)) {
      command.addOption(CliStrings.REBALANCE__INCLUDEREGION, StringUtils.concat(includedRegions,
        StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(excludedRegions)) {
      command.addOption(CliStrings.REBALANCE__EXCLUDEREGION, StringUtils.concat(excludedRegions,
        StringUtils.COMMA_DELIMITER));
    }

    command.addOption(CliStrings.REBALANCE__SIMULATE, String.valueOf(simulate));
    command.addOption(CliStrings.REBALANCE__TIMEOUT, String.valueOf(timeout));

    return new Callable<ResponseEntity<String>>() {
      public ResponseEntity<String> call() throws Exception {
        return new ResponseEntity<String>(processCommand(command.toString()), HttpStatus.OK);
      }
    };
  }

}
