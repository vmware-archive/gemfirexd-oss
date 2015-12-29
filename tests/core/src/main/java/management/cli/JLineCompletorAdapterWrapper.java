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
package management.cli;

import java.util.List;

import org.springframework.shell.core.JLineCompletorAdapter;
import org.springframework.shell.core.Parser;

/**
 * 
 * Wrapper around JLine completor to capture completion 
 * @author tushark
 *
 */
public class JLineCompletorAdapterWrapper extends JLineCompletorAdapter {

  private TestableGfsh shell = null;

  public JLineCompletorAdapterWrapper(Parser parser, TestableGfsh shell) {
    super(parser);
    this.shell = shell;
  }

  @SuppressWarnings("all")
  @Override
  public int complete(final String buffer, final int cursor, final List candidates) {
    Util.debug("Called wrapper adaptor for input string ..." + buffer);
    int i = super.complete(buffer, cursor, candidates);
    shell.addCompletorOutput(buffer, candidates);
    return i;
  }

}
