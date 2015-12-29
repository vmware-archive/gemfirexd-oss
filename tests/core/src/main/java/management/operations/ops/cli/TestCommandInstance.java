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
package management.operations.ops.cli;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import management.util.HydraUtil;
import util.TestException;

public class TestCommandInstance implements Serializable{
  private static final String SPACE = " ";
  private static final String NEWLINE = System.getProperty("line.separator");
  private static final String OPTION_PREFIX = "--";
  private static final String OPTION_VALUE_ASSIGN = "=";

  transient static Random random = new Random();

  public TestCommand testCommand;
  private Map<String, Object> options = new HashMap<String, Object>();
  private Map<String, Object> mandatoryOptions = new HashMap<String, Object>();
  private List<String> arguments = new ArrayList<String>();
  private String mode;

  public static String getRandomSepartor() {

    /*if (random.nextInt(100) > 90)
      return NEWLINE;
    else*/
      return SPACE;
  }

  public String toString() {

    StringBuilder sb = new StringBuilder();
    sb.append(testCommand.command).append(getRandomSepartor());

    // sb.append("A : ").append(SPACE);

    for (String argument : arguments) {
      sb.append(argument).append(getRandomSepartor());
    }

    // sb.append("MO : ").append(SPACE);

    for (String mandatoryOption : mandatoryOptions.keySet()) {
      sb.append(OPTION_PREFIX).append(mandatoryOption).append(OPTION_VALUE_ASSIGN)
          .append(mandatoryOptions.get(mandatoryOption).toString()).append(getRandomSepartor());
    }

    // sb.append("O : ").append(SPACE);

    for (String option : options.keySet()) {
      sb.append(OPTION_PREFIX).append(option).append(OPTION_VALUE_ASSIGN).append(options.get(option).toString())
          .append(getRandomSepartor());
    }

    return sb.toString();
  }

  public void addOption(String optionName, Object value) {
    HydraUtil.logFine("Adding option : " + optionName + " with value : " + value);
    this.options.put(optionName, value);
  }

  public void addMandatoryOption(String optionName, Object value) {
    HydraUtil.logFine("Adding Moption : " + optionName + " with value : " + value);
    this.mandatoryOptions.put(optionName, value);
  }

  public void addArgument(String optionName) {
    throw new TestException("Arguments are deprecated. See 46096");
    /*
    HydraUtil.logFine("Adding argument : " + optionName + " with value : " + optionName);
    this.arguments.add(optionName);*/
  }

  public String getMode() {
    return mode;
  }

  public void setMode(String mode) {
    this.mode = mode;
  }
  
  

}
