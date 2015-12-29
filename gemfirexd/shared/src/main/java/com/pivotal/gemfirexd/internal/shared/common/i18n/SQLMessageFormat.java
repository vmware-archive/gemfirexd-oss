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

package com.pivotal.gemfirexd.internal.shared.common.i18n;

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * Interface to format messageId (as in {@link SQLState}) and arguments into a
 * complete message.
 * 
 * @author swale
 * @since gfxd 1.1
 */
public interface SQLMessageFormat {

  /** Get the message resource bundle name. */
  public String getResourceBundleName();

  /** Get the complete message given an ID in {@link SQLState} and arguments. */
  public String getCompleteMessage(String messageID, Object[] args);
}
