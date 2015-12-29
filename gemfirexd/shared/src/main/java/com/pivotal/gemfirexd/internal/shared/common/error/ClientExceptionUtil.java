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

package com.pivotal.gemfirexd.internal.shared.common.error;

import java.sql.SQLException;
import java.util.Map;

import com.pivotal.gemfirexd.internal.shared.common.i18n.MessageUtil;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * Utility class with static methods to construct {@link SQLException}s from
 * given {@link SQLState}s and optional arguments and cause.
 * 
 * @author swale
 * @since gfxd 1.1
 */
public abstract class ClientExceptionUtil extends ExceptionUtil {

  public static final String CLIENT_MESSAGE_RESOURCE_NAME =
      "com.pivotal.gemfirexd.internal.loc.clientmessages";

  /**
   * The message utility instance we use to find messages It's primed with the
   * name of the client message bundle so that it knows to look there if the
   * message isn't found in the shared message bundle.
   */
  private static final MessageUtil msgutil_;

  static {
    msgutil_ = new MessageUtil(CLIENT_MESSAGE_RESOURCE_NAME);
    setExceptionFactory(new DefaultExceptionFactory30(msgutil_));
  }

  public static void init() {
    // nothing here; just to initialize the static constructor of the class
  }

  protected ClientExceptionUtil() {
    // no instance allowed
  }

  /**
   * This routine provides singleton access to an instance of MessageUtil that
   * is constructed for client messages. It is recommended to use this singleton
   * rather than create your own instance.
   * 
   * The only time you need this instance is if you need to directly format an
   * internationalized message string. In most instances this is done for you
   * when you invoke a SqlException constructor
   * 
   * @return a singleton instance of MessageUtil configured for client messages
   */
  public static MessageUtil getMessageUtil() {
    return msgutil_;
  }

  public static SQLException notImplemented(Object feature) {
    return newSQLException(SQLState.NOT_IMPLEMENTED, null, feature);
  }
}
