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

package com.gemstone.gemfire.internal;

import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * @author kneeraj
 * @since gfxd 1.0.1
 */
public class InsufficientDiskSpaceException extends DiskAccessException implements Sendable {
  private static final long serialVersionUID = -6167707908956900841L;

  public InsufficientDiskSpaceException(String msg, Throwable cause, DiskStore ds) {
    super(msg, cause, ds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void sendTo(DataOutput out) throws IOException {
    // send base DiskAccessException to older versions
    Version peerVersion = InternalDataSerializer.getVersionForDataStream(out);
    if (Version.GFXD_101.compareTo(peerVersion) > 0) {
      DiskAccessException dae = new DiskAccessException(getMessage(),
          getCause());
      InternalDataSerializer.writeSerializableObject(dae, out);
    }
    else {
      InternalDataSerializer.writeSerializableObject(this, out);
    }
  }
}
