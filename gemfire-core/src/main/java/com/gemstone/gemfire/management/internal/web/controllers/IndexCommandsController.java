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

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The IndexCommandsController class implements the REST API calls for the Gfsh Index commands.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.IndexCommands
 * @see com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 7.5
 */
@Controller("indexController")
@RequestMapping("/v1")
@SuppressWarnings("unused")
public class IndexCommandsController extends AbstractCommandsController {

  private static final String DEFAULT_INDEX_TYPE = "range";

  @RequestMapping(value = "/indexes", method = RequestMethod.GET)
  @ResponseBody
  public String listIndex(@RequestParam(value = CliStrings.LIST_INDEX__STATS, defaultValue = "false") final Boolean withStats) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.LIST_INDEX);
    command.addOption(CliStrings.LIST_INDEX__STATS, String.valueOf(Boolean.TRUE.equals(withStats)));
    return processCommand(command.toString());
  }

  @RequestMapping(value = "/indexes", method = RequestMethod.POST)
  @ResponseBody
  public String createIndex(@RequestParam(CliStrings.CREATE_INDEX__NAME) final String name,
                            @RequestParam(CliStrings.CREATE_INDEX__EXPRESSION) final String expression,
                            @RequestParam(CliStrings.CREATE_INDEX__REGION) final String regionNamePath,
                            @RequestParam(value = CliStrings.CREATE_INDEX__GROUP, required = false) final String groupName,
                            @RequestParam(value = CliStrings.CREATE_INDEX__MEMBER, required = false) final String memberNameId,
                            @RequestParam(value = CliStrings.CREATE_INDEX__TYPE, defaultValue = DEFAULT_INDEX_TYPE) final String type)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_INDEX);

    command.addOption(CliStrings.CREATE_INDEX__NAME, name);
    command.addOption(CliStrings.CREATE_INDEX__EXPRESSION, expression);
    command.addOption(CliStrings.CREATE_INDEX__REGION, regionNamePath);
    command.addOption(CliStrings.CREATE_INDEX__TYPE, type);

    if (hasValue(groupName)) {
      command.addOption(CliStrings.CREATE_INDEX__GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.CREATE_INDEX__MEMBER, memberNameId);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/indexes", method = RequestMethod.DELETE)
  @ResponseBody
  public String destroyIndexes(@RequestParam(value = CliStrings.DESTROY_INDEX__GROUP, required = false) final String groupName,
                               @RequestParam(value = CliStrings.DESTROY_INDEX__MEMBER, required = false) final String memberNameId,
                               @RequestParam(value = CliStrings.DESTROY_INDEX__REGION, required = false) final String regionNamePath)
  {
    return internalDestroyIndex(null, groupName, memberNameId, regionNamePath);
  }

  @RequestMapping(value = "/indexes/{name}", method = RequestMethod.DELETE)
  @ResponseBody
  public String destroyIndex(@PathVariable("name") final String indexName,
                             @RequestParam(value = CliStrings.DESTROY_INDEX__GROUP, required = false) final String groupName,
                             @RequestParam(value = CliStrings.DESTROY_INDEX__MEMBER, required = false) final String memberNameId,
                             @RequestParam(value = CliStrings.DESTROY_INDEX__REGION, required = false) final String regionNamePath)
  {
    return internalDestroyIndex(indexName, groupName, memberNameId, regionNamePath);
  }

  protected String internalDestroyIndex(final String indexName,
                                        final String groupName,
                                        final String memberNameId,
                                        final String regionNamePath)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESTROY_INDEX);

    if (hasValue(indexName)) {
      command.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    }

    if (hasValue(groupName)) {
      command.addOption(CliStrings.DESTROY_INDEX__GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.DESTROY_INDEX__MEMBER, memberNameId);
    }

    if (hasValue(regionNamePath)) {
      command.addOption(CliStrings.DESTROY_INDEX__REGION, regionNamePath);
    }

    return processCommand(command.toString());
  }

}
