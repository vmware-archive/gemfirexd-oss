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

package com.pivotal.gemfirexd.internal.impl.sql.compile;

import com.pivotal.gemfirexd.internal.engine.sql.compile.SQLMatcherConstants;

/**
 * Extension to UCode_CharStream for SQLMatcher classes. Currently overrides
 * {@link CharStream#GetImage(int)} to use SQLMatcher constants.
 * 
 */
public final class UCode_MatcherCharStream extends UCode_CharStream {

  public UCode_MatcherCharStream(java.io.Reader dstream, int startline,
      int startcolumn, int buffersize) {
    super(dstream, startline, startcolumn, buffersize);
  }

  @Override
  public final String GetImage(int jjmatchedKind) {

    if (jjmatchedKind == SQLMatcherConstants.STRING
        || jjmatchedKind == SQLMatcherConstants.DELIMITED_IDENTIFIER) {

      if (bufpos >= tokenBegin)
        return new String(buffer, tokenBegin + 1, bufpos - tokenBegin - 1);
      else
        return new String(buffer, tokenBegin + 1, bufsize - tokenBegin - 1)
            + new String(buffer, 0, bufpos + 1);
    }

    if (bufpos >= tokenBegin)
      return new String(buffer, tokenBegin, bufpos - tokenBegin + 1);
    else
      return new String(buffer, tokenBegin, bufsize - tokenBegin)
          + new String(buffer, 0, bufpos + 1);
  }
}
