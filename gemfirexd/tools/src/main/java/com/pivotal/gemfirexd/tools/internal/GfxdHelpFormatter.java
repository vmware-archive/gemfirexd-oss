/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Overrides commons CLI HelpFormatter to change formatting as per GemFireXD.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All Rights Reserved.
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

import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;

import com.gemstone.gemfire.InternalGemFireError;
import com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedResource;

/**
 * Overrides commons CLI {@link HelpFormatter} to change formatting as per
 * GemFireXD (in particular use '=' to separate option and value rather than a
 * space). Like {@link GfxdOption} this will probably better also go into a
 * modified version of commons CLI {@link HelpFormatter} since that class does
 * not look to be designed to be overridden.
 * 
 * @author swale
 * @since 7.0
 */
@SuppressWarnings("unchecked")
public final class GfxdHelpFormatter extends HelpFormatter {

  public GfxdHelpFormatter() {

    /*
     * Comparator used to sort the options when they output in help text
     * 
     * Defaults to case-insensitive alphabetical sorting by option key providing
     * the required options at the start.
     */
    this.optionComparator = new Comparator<Option>() {

      @Override
      public int compare(Option o1, Option o2) {
        final GfxdOption opt1 = (GfxdOption)o1;
        final GfxdOption opt2 = (GfxdOption)o2;

        if (opt1.isRequired()) {
          if (opt2.isRequired()) {
            return opt1.getOptKey().compareToIgnoreCase(opt2.getOptKey());
          }
          else {
            // place required options before non-required ones
            return -1;
          }
        }
        else {
          if (opt2.isRequired()) {
            // place required options before non-required ones
            return 1;
          }
          else {
            return opt1.getOptKey().compareToIgnoreCase(opt2.getOptKey());
          }
        }
      }
    };
  }

  /**
   * Print the cmdLineSyntax to the specified writer, using the specified width.
   * 
   * @param pw
   *          The printWriter to write the help to
   * @param width
   *          The number of characters per line for the usage statement.
   * @param cmdLineSyntax
   *          The usage statement.
   */
  @Override
  public void printUsage(PrintWriter pw, int width, String cmdLineSyntax) {
    final int argPos = cmdLineSyntax.indexOf(' ') + 1;
    final String syntaxPrefix = getSyntaxPrefix();

    printWrapped(pw, width, syntaxPrefix.length() + argPos, syntaxPrefix
        + cmdLineSyntax);
  }

  /**
   * <p>
   * Prints the usage statement for the specified application.
   * </p>
   * 
   * @param pw
   *          The PrintWriter to print the usage statement
   * @param width
   *          The number of characters to display per line
   * @param app
   *          The application name
   * @param options
   *          The command line Options
   * 
   */
  @Override
  public void printUsage(PrintWriter pw, int width, String app,
      final Options options) {
    // initialise the string buffer
    final StringBuilder buff = new StringBuilder(getSyntaxPrefix()).append(app)
        .append(' ');

    // create a list for processed option groups
    final ArrayList<Object> processedGroups = new ArrayList<>();

    // temp variable
    Option option;

    ArrayList<Option> optList = new ArrayList<>(options.getOptions());
    Collections.sort(optList, getOptionComparator());
    // iterate over the options
    for (Iterator<?> i = optList.iterator(); i.hasNext();) {
      // get the next Option
      option = (Option)i.next();

      // check if the option is part of an OptionGroup
      OptionGroup group = options.getOptionGroup(option);

      // if the option is part of a group
      if (group != null) {
        // and if the group has not already been processed
        if (!processedGroups.contains(group)) {
          // add the group to the processed list
          processedGroups.add(group);

          // add the usage clause
          appendOptionGroup(buff, group);
        }
        // otherwise the option was displayed in the group
        // previously so ignore it.
      }
      // if the Option is not part of an OptionGroup
      else {
        appendOption(buff, option, option.isRequired());
      }

      if (i.hasNext()) {
        buff.append(' ');
      }
    }

    // call printWrapped
    printWrapped(pw, width, buff.toString().indexOf(' ') + 1, buff.toString());
  }

  /**
   * Returns the 'syntaxPrefix'.
   */
  @Override
  public String getSyntaxPrefix() {
    return LocalizedResource.getMessage("GFXD_Usage").replace("\n\n", " ");
  }

  /**
   * Appends the usage clause for an OptionGroup to a StringBuilder. The clause
   * is wrapped in square brackets if the group is not required. The display of
   * the options is handled by appendOption
   * 
   * @param buff
   *          the StringBuilder to append to
   * @param group
   *          the group to append
   * @see #appendOption(StringBuffer,Option,boolean)
   */
  protected void appendOptionGroup(final StringBuilder buff,
      final OptionGroup group) {
    if (!group.isRequired()) {
      buff.append('[');
    }

    ArrayList<Option> optList = new ArrayList<>(group.getOptions());
    Collections.sort(optList, getOptionComparator());
    // for each option in the OptionGroup
    for (Iterator<?> i = optList.iterator(); i.hasNext();) {
      // whether the option is required or not is handled at group level
      appendOption(buff, (Option)i.next(), true);

      if (i.hasNext()) {
        buff.append(" | ");
      }
    }

    if (!group.isRequired()) {
      buff.append(']');
    }
  }

  /**
   * Appends the usage clause for an Option to a StringBuffer.
   * 
   * @param buff
   *          the StringBuffer to append to
   * @param option
   *          the Option to append
   * @param required
   *          whether the Option is required or not
   */
  protected void appendOption(final StringBuilder buff, final Option option,
      final boolean required) {
    if (!required) {
      buff.append('[');
    }

    if (option.getOpt() != null) {
      buff.append(getOptPrefix()).append(option.getOpt());
    }
    else {
      buff.append(getLongOptPrefix()).append(option.getLongOpt());
    }

    // if the Option has a value
    if (option.hasArg() && option.hasArgName()) {
      if (option.hasOptionalArg()) {
        buff.append('[');
      }
      if (option.hasValueSeparator()) {
        buff.append(option.getValueSeparator());
      }
      else {
        buff.append(' ');
      }
      buff.append('<').append(option.getArgName()).append('>');
      if (option.hasOptionalArg()) {
        buff.append(']');
      }
    }

    // if the Option is not a required option
    if (!required) {
      buff.append(']');
    }
  }

  /**
   * Render the specified Options and return the rendered Options in a
   * StringBuffer.
   * 
   * @param sb
   *          The StringBuffer to place the rendered Options into.
   * @param width
   *          The number of characters to display per line
   * @param options
   *          The command line Options
   * @param leftPad
   *          the number of characters of padding to be prefixed to each line
   * @param descPad
   *          the number of characters of padding to be prefixed to each
   *          description line
   * 
   * @return the StringBuffer with the rendered Options contents.
   */
  @Override
  protected StringBuffer renderOptions(StringBuffer sb, int width,
      Options options, int leftPad, int descPad) {
    final String lpad = createPadding(leftPad);
    final String dpad = createPadding(descPad);

    // first create list containing only <lpad>-a,--aaa where
    // -a is opt and --aaa is long opt; in parallel look for
    // the longest opt string this list will be then used to
    // sort options ascending
    int max = 0;
    StringBuilder optBuf;
    ArrayList<Object> prefixList = new ArrayList<>();
    List<Option> optList;
    try {
      Method helpOptions = options.getClass().getDeclaredMethod("helpOptions");
      helpOptions.setAccessible(true);
      optList = (List<Option>)helpOptions.invoke(options);
    } catch (Exception e) {
      throw new InternalGemFireError(e);
    }

    Collections.sort(optList, getOptionComparator());

    for (Iterator<?> i = optList.iterator(); i.hasNext();) {
      Option option = (Option)i.next();
      optBuf = new StringBuilder(8);

      if (option.getOpt() == null) {
        optBuf.append(lpad).append("   ").append(getLongOptPrefix())
            .append(option.getLongOpt());
      }
      else {
        optBuf.append(lpad).append(getOptPrefix()).append(option.getOpt());

        if (option.hasLongOpt()) {
          optBuf.append(',').append(getLongOptPrefix())
              .append(option.getLongOpt());
        }
      }

      if (option.hasArg()) {
        if (option.hasArgName()) {
          if (option.hasOptionalArg()) {
            optBuf.append('[');
          }
          if (option.hasValueSeparator()) {
            optBuf.append(option.getValueSeparator());
          }
          else {
            optBuf.append(' ');
          }
          optBuf.append('<').append(option.getArgName()).append('>');
          if (option.hasOptionalArg()) {
            optBuf.append(']');
          }
        }
        else {
          optBuf.append(' ');
        }
      }
      if (option.isRequired()) {
        optBuf.append('*');
      }

      prefixList.add(optBuf);
      max = (optBuf.length() > max) ? optBuf.length() : max;
    }

    int x = 0;
    for (Iterator<?> i = optList.iterator(); i.hasNext();) {
      Option option = (Option)i.next();
      optBuf = new StringBuilder(prefixList.get(x++).toString());

      if (optBuf.length() < max) {
        optBuf.append(createPadding(max - optBuf.length()));
      }

      optBuf.append(dpad);

      int nextLineTabStop = max + descPad;

      if (option.isRequired()) {
        optBuf.append("[")
            .append(LocalizedResource.getMessage("TOOLS_OPTION_REQUIRED"))
            .append("] ");
      }

      if (option.getDescription() != null) {
        optBuf.append(option.getDescription());
      }

      renderWrappedText(sb, width, nextLineTabStop, optBuf.toString());

      if (i.hasNext()) {
        sb.append(getNewLine());
      }
    }

    return sb;
  }
}
