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
package com.gemstone.gemfire.pdx.internal;

import java.nio.ByteBuffer;

/**
 * Used by {@link PdxInstanceImpl#equals(Object)} to act as if it has
 * a field whose value is always the default.
 * @author darrel
 *
 */
public class DefaultPdxField extends PdxField {

  public DefaultPdxField(PdxField f) {
    super(f);
  }

  public ByteBuffer getDefaultBytes() {
    return getFieldType().getDefaultBytes();
  }

}
