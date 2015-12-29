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
package com.gemstone.gemfire.management.internal.cli;

import java.util.Collections;
import java.util.Map;

import com.gemstone.gemfire.internal.lang.StringUtils;

/**
 * The CommandRequest class encapsulates information pertaining to the command the user entered in Gfsh.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.GfshParseResult
 * @since 7.5
 */
@SuppressWarnings("unused")
public class CommandRequest {

  protected static final String OPTION_SPECIFIER = "--";

  private final byte[][] fileData;

  private final GfshParseResult parseResult;

  private final Map<String, String> env;

  private String customInput;

  public CommandRequest(final Map<String, String> env) {
    this.parseResult = null;
    this.env = env;
    this.fileData = null;
  }

  public CommandRequest(final Map<String, String> env, final byte[][] fileData) {
    this.parseResult = null;
    this.env = env;
    this.fileData = fileData;
  }

  public CommandRequest(final GfshParseResult parseResult, final Map<String, String> env) {
    this(parseResult, env, null);
  }

  public CommandRequest(final GfshParseResult parseResult, final Map<String, String> env, final byte[][] fileData) {
    assert parseResult != null : "The Gfsh ParseResult cannot be null!";
    assert env != null : "The reference to the Gfsh CLI environment cannot be null!";
    this.parseResult = parseResult;
    this.env = env;
    this.fileData = fileData;
  }

  public String getName() {
    if (getUserInput() != null) {
      final String[] userInputTokenized = getUserInput().split("\\s");
      final StringBuilder buffer = new StringBuilder();

      for (final String token : userInputTokenized) {
        if (!token.startsWith(OPTION_SPECIFIER)) {
          buffer.append(token).append(StringUtils.SPACE);
        }
      }

      return buffer.toString().trim();
    }
    else {
      return "unknown";
    }
  }

  public String getCustomInput() {
    return customInput;
  }

  public void setCustomInput(final String input) {
    this.customInput = input;
  }

  public Map<String, String> getEnvironment() {
    return Collections.unmodifiableMap(env);
  }

  public byte[][] getFileData() {
    return fileData;
  }

  public boolean hasFileData() {
    return (getFileData() != null);
  }

  public String getInput() {
    return StringUtils.defaultIfBlank(getCustomInput(), getUserInput());
  }

  public Map<String, String> getParameters() {
    return getParseResult().getParamValueStrings();
  }

  protected GfshParseResult getParseResult() {
    return parseResult;
  }

  public String getUserInput() {
    return getParseResult().getUserInput();
  }

}
