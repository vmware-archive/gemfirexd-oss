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
package com.company.data;
/**
 * A <code>Declarable</code> <code>ObjectSizer</code> for used for XML testing
 *
 * @author Mitch Thomas
 * @since 5.0
 */
import com.gemstone.gemfire.internal.util.Sizeof;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.cache.Declarable;
import java.util.Properties;

public class MySizer implements ObjectSizer, Declarable {

  String name;

  public int sizeof( Object o ) {
    return Sizeof.sizeof(o);
  }

  public void init(Properties props) {
      this.name = props.getProperty("name", "defaultName");
  }
}
