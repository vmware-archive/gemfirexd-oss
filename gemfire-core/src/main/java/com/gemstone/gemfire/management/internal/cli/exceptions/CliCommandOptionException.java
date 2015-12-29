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
package com.gemstone.gemfire.management.internal.cli.exceptions;

import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;
import com.gemstone.gemfire.management.internal.cli.parser.Option;
import com.gemstone.gemfire.management.internal.cli.parser.OptionSet;

public class CliCommandOptionException extends CliCommandException {

  private Option option;

  /**
   * @return option for which the exception occured
   */
  public Option getOption() {
    return option;
  }

  /**
   * @param option
   *          the option to set
   */
  public void setOption(Option option) {
    this.option = option;
  }

  public CliCommandOptionException(CommandTarget commandTarget, Option option) {
    super(commandTarget);
    this.setOption(option);
  }

  public CliCommandOptionException(CommandTarget commandTarget, Option option,
      OptionSet optionSet) {
    super(commandTarget, optionSet);
    this.setOption(option);
  }

}
