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
/**
 * 
 */
package util;

import com.gemstone.gemfire.GemFireRethrowable;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.PdxWriter;

import hydra.HydraRuntimeException;
import hydra.Log;

import java.io.IOException;
import java.util.Properties;

import pdx.PdxTest;

/**
 * @author lynn
 *
 */
public class PdxTestSerializer implements PdxSerializer, Declarable {

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializer#toData(java.lang.Object, com.gemstone.gemfire.pdx.PdxWriter)
   */
  public boolean toData(Object o, PdxWriter out) throws GemFireRethrowable {
    boolean returnValue;
    String className = o.getClass().getName();
    if (className.equals("util.VersionedValueHolder") ||
        className.equals("util.VersionedQueryObject") ||
        className.equals("parReg.query.VersionedNewPortfolio") ||
        className.equals("objects.VersionedPortfolio")) {
      PdxTest.invokeToData(o, out);
      returnValue = true;
    } else {
      returnValue = false;
    }
    Log.getLogWriter().info("In " + this.getClass().getName() + ".toData with " + 
        o.getClass().getName() + ", returning " + returnValue);
    return returnValue;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.pdx.PdxSerializer#fromData(java.lang.Object, com.gemstone.gemfire.pdx.PdxReader)
   */
  public Object fromData(Class<?> clazz, PdxReader in) {
    String className = clazz.getName();
    Object o;
    if (className.equals("util.VersionedValueHolder") ||
        className.equals("util.VersionedQueryObject") ||
        className.equals("parReg.query.VersionedNewPortfolio") ||
        className.equals("objects.VersionedPortfolio")) {
      try {
        o = clazz.newInstance();
      } catch (InstantiationException e) {
        throw new HydraRuntimeException("Unable to instantiate class " + clazz, e);
      } catch (IllegalAccessException e) {
        throw new HydraRuntimeException("Unable to instantiate class " + clazz, e);
      }
      PdxTest.invokeFromData(o, in);
    } else {
      o = null;
    }
    Log.getLogWriter().info("In " + this.getClass().getName() + ".fromData with " + 
        className + ", returning " + o);
    return o;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    
  }

}
