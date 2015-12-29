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

import java.io.IOException;

import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandExecutionContext;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.management.internal.web.util.ConvertUtils;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

/**
 * The DeployCommandsController class implements the GemFire Management REST API web service endpoints for the
 * Gfsh Deploy Commands.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.ConfigCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 7.5
 */
@Controller("deployController")
@RequestMapping("/v1")
@SuppressWarnings("unused")
public class DeployCommandsController extends AbstractCommandsController {

  protected static final String RESOURCES_REQUEST_PARAMETER = "resources";

  @RequestMapping(value = "/deployed", method = RequestMethod.GET)
  @ResponseBody
  public String listDeployed(@RequestParam(value = CliStrings.LIST_DEPLOYED__GROUP, required = false) final String[] groups) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.LIST_DEPLOYED);

    if (hasValue(groups)) {
      command.addOption(CliStrings.LIST_DEPLOYED__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/deployed", method = RequestMethod.POST)
  @ResponseBody
  // final MultipartHttpServletRequest request
  // @RequestPart(RESOURCES_REQUEST_PARAMETER) final Resource[] jarFileResources,
  public String deploy(@RequestParam(RESOURCES_REQUEST_PARAMETER) final MultipartFile[] jarFileResources,
                       @RequestParam(value = CliStrings.DEPLOY__GROUP, required = false) final String[] groupNames,
                       @RequestParam(value = CliStrings.DEPLOY__JAR, required = false) final String jarFileName,
                       @RequestParam(value = CliStrings.DEPLOY__DIR, required = false) final String directory)
    throws IOException
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.DEPLOY);

    CommandExecutionContext.setBytesFromShell(ConvertUtils.convert(jarFileResources));

    if (hasValue(groupNames)) {
      command.addOption(CliStrings.DEPLOY__GROUP, StringUtils.concat(groupNames, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(jarFileName)) {
      command.addOption(CliStrings.DEPLOY__JAR, jarFileName);
    }

    if (hasValue(directory)) {
      command.addOption(CliStrings.DEPLOY__DIR, directory);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(value = "/deployed", method = RequestMethod.DELETE)
  @ResponseBody
  public String undeploy(@RequestParam(value = CliStrings.UNDEPLOY__GROUP, required = false) final String[] groups,
                         @RequestParam(value = CliStrings.UNDEPLOY__JAR, required = false) final String[] jarFileNames) {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.UNDEPLOY);

    if (hasValue(groups)) {
      command.addOption(CliStrings.UNDEPLOY__GROUP, StringUtils.concat(groups, StringUtils.COMMA_DELIMITER));
    }

    if (hasValue(jarFileNames)) {
      command.addOption(CliStrings.UNDEPLOY__JAR, StringUtils.concat(jarFileNames, StringUtils.COMMA_DELIMITER));
    }

    return processCommand(command.toString());
  }

}
