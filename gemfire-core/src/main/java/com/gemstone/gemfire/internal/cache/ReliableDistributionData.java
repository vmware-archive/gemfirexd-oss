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
package com.gemstone.gemfire.internal.cache;

import java.util.*;

/**
 * Represents one or more distributed operations that can be reliably distributed.
 * This interface allows the data to be queued and checked for reliable distribution.
 * 
 * @author Darrel Schneider
 * @since 5.0
 */
public interface ReliableDistributionData  {
//  /**
//   * Returns a set of the recipients that this data was sent to successfully.
//   * @param processor the reply processor used for responses to this data.
//   */
//  public Set getSuccessfulRecipients(ReliableReplyProcessor21 processor);
  /**
   * Returns the number of logical operations this data contains.
   */
  public int getOperationCount();
  /**
   * Returns a list of QueuedOperation instances one for each logical
   * operation done by this data instance.
   */
  public List getOperations();
}
