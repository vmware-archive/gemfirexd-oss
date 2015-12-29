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

import java.util.List;

/**
 * Interface used for implementing an event handler that is invoked
 * asynchronously. Installed by calling the "CREATE ASYNCEVENTLISTENER" DDL.
 * 
 * @author Asif, Yogesh
 */
public interface AsyncEventListener {

  /**
   * Process the list of <code>Event</code>s. This method will be invoked
   * asynchronously when events are queued for processing. For
   * AsyncEventListener, the {@link Event#getOldRow()} will always return null
   * indicating that old value is not available. {@link Event#getNewRow()} will
   * return the column values for a row created. For update operations it will
   * only return meaningful values for modified columns and null for unmodified
   * columns. The positions of columns modified can be obtained from
   * {@link Event#getModifiedColumns()}
   * 
   * @param events
   *          The list of <code>Event</code>s to process
   * @return whether the events were successfully processed. If returned false,
   *         the events will not be deleted from the internal queue.
   */
  public boolean processEvents(List<Event> events);

  /**
   * This is invoked when the AsyncEventListener configuration is stopped. User
   * can do any necessary cleanup in this method.
   */
  public void close();

  /**
   * The AsyncListener implemented by the user can be initialized via this
   * method. This is invoked after the creation of an instance of the class
   * during the initialization of AsyncEventListener configuration. This can be
   * used to populate fields which can be used during processing of the events.
   * init is invoked only once, after configuring the AsyncEventListener. 
   * 
   * @param initParamStr
   *          String containing initialisation parameters for the
   *          AsyncEventListener
   */
  public void init(String initParamStr);
 
  /**
   * This is invoked just prior to starting the async invocation thread for processing the events.
   * User can do any  initialisation like creating the file handles etc , which would be used in 
   * processing of events. 
   */
  public void start();

}
