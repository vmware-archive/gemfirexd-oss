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

import java.io.DataOutputStream;
import java.io.OutputStream;

import com.gemstone.gemfire.internal.shared.Version;

/**
 * An extension of {@link DataOutputStream} that implements
 * {@link VersionedDataStream}.
 * 
 * @author swale
 * @since 7.1
 */
public final class VersionedDataOutputStream extends DataOutputStream implements
    VersionedDataStream {

  private final Version version;

  /**
   * Creates a VersionedDataOutputStream that wraps the specified underlying
   * OutputStream.
   * 
   * @param out
   *          the underlying output stream
   * @param version
   *          the product version that serialized object on the given
   *          {@link OutputStream}
   */
  public VersionedDataOutputStream(OutputStream out, Version version) {
    super(out);
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Version getVersion() {
    return this.version;
  }
}
