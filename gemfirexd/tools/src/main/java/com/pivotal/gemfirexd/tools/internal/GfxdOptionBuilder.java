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
 * Adapted from Apache Commons CLI source for GemFireXD.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

/**
 * OptionBuilder allows the user to create Options using descriptive methods.
 * 
 * <p>
 * Details on the Builder pattern can be found at <a
 * href="http://c2.com/cgi-bin/wiki?BuilderPattern">
 * http://c2.com/cgi-bin/wiki?BuilderPattern</a>.
 * </p>
 * 
 * @author John Keyes (john at integralsource.com)
 * @version $Revision: 754830 $, $Date: 2009-03-16 00:26:44 -0700 (Mon, 16 Mar
 *          2009) $
 * @since 1.0
 */
public final class GfxdOptionBuilder {
  /** long option */
  private String longopt;

  /** option description */
  private String description;

  /** argument name */
  private String argName;

  /** is required? */
  private boolean required;

  /** the number of arguments */
  private int numberOfArgs = GfxdOption.UNINITIALIZED;

  /** option type */
  private Object type;

  /** option can have an optional argument value */
  private boolean optionalArg;

  /** value separator for argument value */
  private char valuesep;

  public GfxdOptionBuilder() {
  }

  /**
   * Resets the member variables to their default values.
   */
  private void reset() {
    description = null;
    argName = "arg";
    longopt = null;
    type = null;
    required = false;
    numberOfArgs = GfxdOption.UNINITIALIZED;

    // PMM 9/6/02 - these were missing
    optionalArg = false;
    valuesep = (char)0;
  }

  /**
   * The next Option created will have the following long option value.
   * 
   * @param newLongopt
   *          the long option value
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder withLongOpt(String newLongopt) {
    this.longopt = newLongopt;
    return this;
  }

  /**
   * The next Option created will require an argument value.
   * 
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder hasArg() {
    this.numberOfArgs = 1;
    return this;
  }

  /**
   * The next Option created will require an argument value if
   * <code>hasArg</code> is true.
   * 
   * @param hasArg
   *          if true then the Option has an argument value
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder hasArg(boolean hasArg) {
    this.numberOfArgs = hasArg ? 1 : GfxdOption.UNINITIALIZED;
    return this;
  }

  /**
   * The next Option created will have the specified argument value name.
   * 
   * @param name
   *          the name for the argument value
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder withArgName(String name) {
    this.argName = name;
    return this;
  }

  /**
   * The next Option created will be required.
   * 
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder isRequired() {
    this.required = true;
    return this;
  }

  /**
   * The next Option created uses <code>sep</code> as a means to separate
   * argument values.
   * 
   * <b>Example:</b>
   * 
   * <pre>
   * Option opt = this.withValueSeparator(':').create('D');
   * 
   * CommandLine line = parser.parse(args);
   * 
   * String propertyName = opt.getValue(0);
   * String propertyValue = opt.getValue(1);
   * </pre>
   * 
   * @param sep
   *          The value separator to be used for the argument values.
   * 
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder withValueSeparator(char sep) {
    this.valuesep = sep;
    return this;
  }

  /**
   * The next Option created uses '<code>=</code>' as a means to separate
   * argument values.
   * 
   * <b>Example:</b>
   * 
   * <pre>
   * Option opt = this.withValueSeparator().create('D');
   * 
   * CommandLine line = parser.parse(args);
   * 
   * String propertyName = opt.getValue(0);
   * String propertyValue = opt.getValue(1);
   * </pre>
   * 
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder withValueSeparator() {
    this.valuesep = '=';
    return this;
  }

  /**
   * The next Option created will be required if <code>required</code> is true.
   * 
   * @param newRequired
   *          if true then the Option is required
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder isRequired(boolean newRequired) {
    this.required = newRequired;
    return this;
  }

  /**
   * The next Option created can have unlimited argument values.
   * 
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder hasArgs() {
    this.numberOfArgs = GfxdOption.UNLIMITED_VALUES;
    return this;
  }

  /**
   * The next Option created can have <code>num</code> argument values.
   * 
   * @param num
   *          the number of args that the option can have
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder hasArgs(int num) {
    this.numberOfArgs = num;
    return this;
  }

  /**
   * The next Option can have an optional argument.
   * 
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder hasOptionalArg() {
    this.numberOfArgs = 1;
    this.optionalArg = true;
    return this;
  }

  /**
   * The next Option can have an unlimited number of optional arguments.
   * 
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder hasOptionalArgs() {
    this.numberOfArgs = GfxdOption.UNLIMITED_VALUES;
    this.optionalArg = true;
    return this;
  }

  /**
   * The next Option can have the specified number of optional arguments.
   * 
   * @param numArgs
   *          - the maximum number of optional arguments the next Option created
   *          can have.
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder hasOptionalArgs(int numArgs) {
    this.numberOfArgs = numArgs;
    this.optionalArg = true;
    return this;
  }

  /**
   * The next Option created will have a value that will be an instance of
   * <code>type</code>.
   * 
   * @param newType
   *          the type of the Options argument value
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder withType(Object newType) {
    this.type = newType;
    return this;
  }

  /**
   * The next Option created will have the specified description
   * 
   * @param newDescription
   *          a description of the Option's purpose
   * @return the OptionBuilder instance
   */
  public GfxdOptionBuilder withDescription(String newDescription) {
    this.description = newDescription;
    return this;
  }

  /**
   * Create an Option using the current settings and with the specified Option
   * <code>char</code>.
   * 
   * @param opt
   *          the character representation of the Option
   * @return the Option instance
   * @throws IllegalArgumentException
   *           if <code>opt</code> is not a valid character. See Option.
   */
  public GfxdOption create(char opt) throws IllegalArgumentException {
    return create(String.valueOf(opt));
  }

  /**
   * Create an Option using the current settings
   * 
   * @return the Option instance
   * @throws IllegalArgumentException
   *           if <code>longOpt</code> has not been set.
   */
  public GfxdOption create() throws IllegalArgumentException {
    if (longopt == null) {
      this.reset();
      throw new IllegalArgumentException("must specify longopt");
    }

    return create(null);
  }

  /**
   * Create an Option using the current settings and with the specified Option
   * <code>char</code>.
   * 
   * @param opt
   *          the <code>java.lang.String</code> representation of the Option
   * @return the Option instance
   * @throws IllegalArgumentException
   *           if <code>opt</code> is not a valid character. See Option.
   */
  public GfxdOption create(String opt) throws IllegalArgumentException {
    GfxdOption option = null;
    try {
      // create the option
      option = new GfxdOption(opt, description);

      // set the option properties
      option.setLongOpt(longopt);
      option.setRequired(required);
      option.setOptionalArg(optionalArg);
      option.setArgs(numberOfArgs);
      option.setType(type);
      option.setValueSeparator(valuesep);
      option.setArgName(argName);
    } finally {
      // reset the OptionBuilder properties
      this.reset();
    }

    // return the Option instance
    return option;
  }
}
