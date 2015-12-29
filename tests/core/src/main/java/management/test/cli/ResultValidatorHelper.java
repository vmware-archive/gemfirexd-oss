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
package management.test.cli;

import hydra.Log;

import java.util.Arrays;

import org.springframework.util.StringUtils;

/**
 * This is helper class containing different kind of Result Validator.
 * @author vbalaut
 *
 */
public class ResultValidatorHelper {
  public static ResultValidator shouldNotHaveValidator(final String arg) {
    return new ResultValidator() {

      @Override
      public Status validate(String output) {
        Log.getLogWriter().fine("Validating command output for not having :" + arg);
        if(output.indexOf(arg) > 0) {
          return Status.ERROR;
        }
        return Status.SUCCESS;
      }

      @Override
      public String getMessage() {
        return "Command output should not have any occurance of " + arg;
      }
    };
  }

  public static  ResultValidator noOfTimesOccuranceValidator(final int occurance, final String arg) {
    return new ResultValidator() {
      int count = 0;
      @Override
      public Status validate(String output) {
        Log.getLogWriter().info("Validating command output for having :" + arg + " " + occurance + " times...");
        count = StringUtils.countOccurrencesOf(output, arg);
        if(count != occurance) {
          return Status.ERROR;
        }
        return Status.SUCCESS;
      }

      @Override
      public String getMessage() {
        return "We were expecting " + occurance + " occurance of " + arg  + " in command output but we got it " + count + " times.";
      }
    };
  }

  public static ResultValidator onlyOneOccuranceValidator(final String... args) {
    return new ResultValidator() {

      private String failedString;
      int count = 0;
      @Override
      public Status validate(String output) {
        Log.getLogWriter().info("Validating command output for having :" + Arrays.toString(args) + " only once...");
        for (String arg : args) {
          count = StringUtils.countOccurrencesOf(output, arg);
          if (count != 1) {
            failedString = arg;
            return Status.ERROR;
          }
        }
        return Status.SUCCESS;
      }

      @Override
      public String getMessage() {
        return "We were expecting " + failedString + " to occur only once and we got it " + count + " times.";
      }
    };
  }

  public static ResultValidator noOfLinesValidatorForTabularOutut(final int expectedNoOfLines) {
    return noOfLinesValidator(expectedNoOfLines, 2);
  }

  public static ResultValidator noOfLinesValidator(final int expectedNoOfLines) {
    return noOfLinesValidator(expectedNoOfLines, 0);
  }

  public static ResultValidator noOfLinesValidator(final int expectedNoOfLines, final int ignoreLineCount) {
    return new ResultValidator() {
      int count = 0;

      @Override
      public Status validate(String output) {
        Log.getLogWriter().info("Validating command output for " + expectedNoOfLines + " no of lines..");
        String[] lines = output.split("\n");
        count = lines.length - ignoreLineCount;
        if (count != expectedNoOfLines) {
          return Status.ERROR;
        }
        return Status.SUCCESS;
      }

      @Override
      public String getMessage() {
        return "We were expecting " + expectedNoOfLines + " lines but we got " + count + ".";
      }
    };
  }
  
  public static  ResultValidator verifyLine(final char separator, final String... args ) {
    return new ResultValidator() {

      @Override
      public Status validate(String output) {
        Log.getLogWriter().fine("Validating command output for line :" + Arrays.toString(args) + "...");
        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
          sb.append(arg).append(separator);
        }
        sb.deleteCharAt(sb.length() - 1);
        String toMatchString = sb.toString();
        toMatchString = toMatchString.replaceAll("( )+", "");
        output = output.replaceAll("( )+", "");
        Log.getLogWriter().info(output);
        Log.getLogWriter().info(toMatchString);
        int index = output.indexOf(toMatchString);
        if (index < 0) {
          return Status.ERROR;
        }
        return Status.SUCCESS;
      }

      @Override
      public String getMessage() {
        return "We were expecting " + Arrays.toString(args) + " but we did not get anything of that sort in the command output.";
      }
    };
  }

  public static  ResultValidator verifyLineForDescribe(final String... args) {
    return verifyLine(':', args);
  }
  
  public static  ResultValidator verifyLineForTabularData(final String... args) {
    return verifyLine('|', args);
  }

  public static  ResultValidator shouldNotHaveValidatorWithIgnoreCase(final String... args) {
    return new ResultValidator() {

      private String failedString;

      @Override
      public Status validate(String output) {
        output = output.toLowerCase();
        Log.getLogWriter().fine("Validating command output for not having :" + Arrays.toString(args));
        for (String arg : args) {
          if (output.indexOf(arg.toLowerCase()) > 0) {
            failedString = arg;
            return Status.ERROR;
          }
        }
        return Status.SUCCESS;
      }

      @Override
      public String getMessage() {
        return "Command output should not have any occurance of " + failedString + ".";
      }
    };
  }
}
