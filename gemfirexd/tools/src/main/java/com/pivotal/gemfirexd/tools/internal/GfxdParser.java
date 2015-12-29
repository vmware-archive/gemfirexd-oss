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

package com.pivotal.gemfirexd.tools.internal;

import java.util.ArrayList;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;

/**
 * A {@link Parser} implementation for GemFireXD that correctly handles a value
 * separator that is not a '='. This allows using a more generic property kind
 * like -J-D&lt;property=value&gt;.
 * 
 * @author swale
 * @since 7.0
 */
public class GfxdParser extends Parser {

  /**
   * This flatten method does so using the following rules:
   * <ol>
   * <li>If an {@link Option} exists for the first character of the
   * <code>arguments</code> entry <b>AND</b> an {@link Option} does not exist
   * for the whole <code>argument</code> then add the first character as an
   * option to the processed tokens list e.g. "-D" and add the rest of the entry
   * to the also.</li>
   * <li>Otherwise just add the token to the processed tokens list.</li>
   * </ol>
   * 
   * @param options
   *          The Options to parse the arguments by.
   * @param arguments
   *          The arguments that have to be flattened.
   * @param stopAtNonOption
   *          specifies whether to stop flattening when a non option has been
   *          encountered
   * 
   * @return a String array of the flattened arguments
   */
  @Override
  protected String[] flatten(Options options, String[] arguments,
      boolean stopAtNonOption) {
    ArrayList<String> tokens = new ArrayList<String>();

    boolean eatTheRest = false;

    for (int i = 0; i < arguments.length; i++) {
      String arg = arguments[i];

      if ("--".equals(arg)) {
        eatTheRest = true;
        tokens.add("--");
      }
      else if ("-".equals(arg)) {
        tokens.add("-");
      }
      else if (arg.length() > 0 && arg.charAt(0) == '-') {
        final String opt;
        if (arg.length() > 1 && arg.charAt(1) == '-') {
          opt = arg.substring(2);
        }
        else {
          opt = arg.substring(1);
        }

        if (options.hasOption(opt)) {
          tokens.add(arg);
        }
        else {
          int sepIdx = opt.indexOf('=');
          if (sepIdx >= 0 && options.hasOption(opt.substring(0, sepIdx))) {
            // the format is --foo=value or -foo=value
            sepIdx = arg.indexOf('=');
            tokens.add(arg.substring(0, sepIdx)); // --foo
            tokens.add(arg.substring(sepIdx + 1)); // value
          }
          else if (options.hasOption(arg.substring(0, 2))) {
            // the format is a special properties option (-Dproperty=value)
            tokens.add(arg.substring(0, 2)); // -D
            tokens.add(arg.substring(2)); // property=value
          }
          else {
            // search for custom value separator through the options
            Option foundOption = null;
            for (Object optionObj : options.getOptions()) {
              final Option option = (Option)optionObj;
              if (option.hasValueSeparator()
                  && option.getValueSeparator() != '='
                  && (sepIdx = opt.indexOf(option.getValueSeparator())) >= 0) {
                final String optKey = opt.substring(0, sepIdx);
                if (optKey.equals(option.getOpt())
                    || optKey.equals(option.getOpt())) {
                  foundOption = option;
                  break;
                }
              }
            }
            if (foundOption != null) {
              // the format is --foo<sep>value or -foo<sep>value
              sepIdx = arg.indexOf(foundOption.getValueSeparator());
              tokens.add(arg.substring(0, sepIdx)); // --foo
              tokens.add(arg.substring(sepIdx + 1)); // value
            }
            else {
              eatTheRest = stopAtNonOption;
              tokens.add(arg);
            }
          }
        }
      }
      else {
        tokens.add(arg);
      }

      if (eatTheRest) {
        for (i++; i < arguments.length; i++) {
          tokens.add(arguments[i]);
        }
      }
    }

    return tokens.toArray(new String[tokens.size()]);
  }
}
