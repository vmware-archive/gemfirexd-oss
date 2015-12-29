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
package com.gemstone.gemfire.management.internal.cli.shell;

import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.ResultBuilder;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

/**
 * 
 * @author Abhishek Chaudhari
 */
public class GfshDUnitTest extends CliCommandTestBase {
  private static final long serialVersionUID = 1L;

  public GfshDUnitTest(String name) {
    super(name);
  }

  public void testMultiLineCommand() {
    createDefaultSetup(null);

    // Execute a command
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_MEMBER);
    csb.addOption(CliStrings.LIST_MEMBER__GROUP, "nogroup");
    CommandResult cmdResult = executeCommand(csb.getCommandString());

    assertNotNull("Command Result was null.", cmdResult);
    assertTrue("Command Result is not OK.", cmdResult.getStatus().equals(Result.Status.OK));

    cmdResult.resetToFirstLine();
    String resultAsString = ResultBuilder.resultAsString(cmdResult);
    resultAsString = resultAsString.trim();
    assertEquals("Unexpected result.", CliStrings.NO_MEMBERS_FOUND_MESSAGE, resultAsString);
    
    // Now execute same command with a new Continuation on new line
    csb = new CommandStringBuilder(CliStrings.LIST_MEMBER).addNewLine()
                  .addOption(CliStrings.LIST_MEMBER__GROUP, "nogroup");

    cmdResult = executeCommand(csb.getCommandString());

    assertNotNull("Command Result was null.", cmdResult);
    assertTrue("Command Result is not OK.", cmdResult.getStatus().equals(Result.Status.OK));
    cmdResult.resetToFirstLine();
    resultAsString = ResultBuilder.resultAsString(cmdResult);
    resultAsString = resultAsString.trim();
    assertEquals("Unexpected result.", CliStrings.NO_MEMBERS_FOUND_MESSAGE, resultAsString);
  }
}
