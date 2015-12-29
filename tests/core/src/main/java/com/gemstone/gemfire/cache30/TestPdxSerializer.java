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
package com.gemstone.gemfire.cache30;

import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.PdxWriter;

/**
 * @author dsmith
 *
 */
public class TestPdxSerializer implements PdxSerializer, Declarable2 {

  private Properties properties;

  public boolean toData(Object o, PdxWriter out) {
    return false;
  }

  public Object fromData(Class<?> clazz, PdxReader in) {
    return null;
  }

  public void init(Properties props) {
    this.properties = props;
    
  }

  public Properties getConfig() {
    return properties;
  }

  @Override
  public int hashCode() {
    return properties.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof TestPdxSerializer)) {
      return false;
    }
    return properties.equals(((TestPdxSerializer)obj).properties);
  }
  
  
  

}
