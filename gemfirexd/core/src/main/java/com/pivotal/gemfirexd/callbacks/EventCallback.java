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

package com.pivotal.gemfirexd.callbacks;

import java.sql.SQLException;

/**
 * Interface used by implementations of the "CLASSNAME" given in
 * "SYS.ADD_LISTENER" and "SYS.ATTACH_WRITER" procedures.
 */
public interface EventCallback {

  /**
   * This method is invoked when the callback is fired on a table event.
   *
   * @param event
   *          a {@link Event} object that provides details of the event
   *          including the old row, update row etc.
   *
   * @throws SQLException
   *          On failure. If an exception is thrown in a writer then the
   *          current update is aborted. If an exception is thrown in a
   *          listener then the update goes through but exception is
   *          propagated back to the originating node.
   */
  void onEvent(Event event) throws SQLException;

  /**
   * Any cleanup required when the callback is destroyed should be done here.
   *
   * @throws SQLException on error
   */
  void close() throws SQLException;

  /**
   * Initialize this callback with parameters provided in the form of given
   * string. This is invoked immediately after the creation of the callback
   * using its default constructor.
   * 
   * @param initStr
   *          String used to initialize this callback
   * 
   * @throws SQLException
   *           on an error during initialization
   */
  void init(String initStr) throws SQLException;
}
