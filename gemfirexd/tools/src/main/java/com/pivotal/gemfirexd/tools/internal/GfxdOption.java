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

import java.lang.reflect.Field;

import org.apache.commons.cli.Option;

import com.gemstone.gemfire.InternalGemFireError;

/**
 * A dirty hack to override commons Option for GemFireXD to allow for '-' in
 * option names. We should ship a modified commons-cli with source instead.
 * 
 * @author swale
 * @since 7.0
 */
public class GfxdOption extends Option {

  private static final long serialVersionUID = 1L;

  private static class OptFieldInitializer {
    private static final Field optField;
    static {
      try {
        optField = Option.class.getDeclaredField("opt");
        optField.setAccessible(true);
      } catch (Exception e) {
        throw new ExceptionInInitializerError(e);
      }
    }
  }

  public GfxdOption(String opt, String description)
      throws IllegalArgumentException {
    this(opt, null, false, description);
  }

  /**
   * Creates an Option using the specified parameters.
   * 
   * @param opt
   *          short representation of the option
   * @param hasArg
   *          specifies whether the Option takes an argument or not
   * @param description
   *          describes the function of the option
   * 
   * @throws IllegalArgumentException
   *           if there are any non valid Option characters in <code>opt</code>.
   */
  public GfxdOption(String opt, boolean hasArg, String description)
      throws IllegalArgumentException {
    this(opt, null, hasArg, description);
  }

  /**
   * Creates an Option using the specified parameters.
   * 
   * @param opt
   *          short representation of the option
   * @param longOpt
   *          the long representation of the option
   * @param hasArg
   *          specifies whether the Option takes an argument or not
   * @param description
   *          describes the function of the option
   * 
   * @throws IllegalArgumentException
   *           if there are any non valid Option characters in <code>opt</code>.
   */
  public GfxdOption(String opt, String longOpt, boolean hasArg,
      String description) throws IllegalArgumentException {
    super(opt.replace('-', '_'), longOpt, hasArg, description);
    try {
      OptFieldInitializer.optField.set(this, opt);
    } catch (Exception ex) {
      throw new InternalGemFireError(ex);
    }
  }

  /**
   * Get a single value for this option if the given option string matches this
   * option (either long or short).
   */
  public final String getOptionValue(String opt) {
    if (opt.equals(getOpt()) || opt.equals(getLongOpt())) {
      final String val = getValue();
      return val != null ? val : "";
    }
    return null;
  }

  /**
   * Returns the 'unique' Option identifier.
   * 
   * @return the 'unique' Option identifier
   */
  public final String getOptKey() {
    final String opt = getOpt();
    if (opt != null) {
      return opt;
    }
    // if 'opt' is null, then it is a 'long' option
    return getLongOpt();
  }
}
