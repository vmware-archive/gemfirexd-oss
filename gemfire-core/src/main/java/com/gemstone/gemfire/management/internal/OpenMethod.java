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
package com.gemstone.gemfire.management.internal;

import java.io.InvalidObjectException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import javax.management.openmbean.OpenDataException;

import com.sun.jmx.mbeanserver.DefaultMXBeanMappingFactory;
import com.sun.jmx.mbeanserver.MXBeanMapping;
import com.sun.jmx.mbeanserver.MXBeanMappingFactory;

/**
 * Top level class to convert java types to open types and vice-versa
 * using Sun internal classes.
 *
 * @author rishim
 */
public final class OpenMethod {

  private final MXBeanMapping[] paramConverters;
  private final MXBeanMapping returnValConverter;

  public OpenMethod(Method m) {
    try {
      final MXBeanMappingFactory mapping = MXBeanMappingFactory.DEFAULT;
      final Type[] params = m.getGenericParameterTypes();
      final MXBeanMapping[] paramConverters = new MXBeanMapping[params.length];
      boolean skipParamsConversion = true;
      // using reflection since isIdentity method is package protected
      final Method identityCheck = DefaultMXBeanMappingFactory.class
          .getDeclaredMethod("isIdentity", MXBeanMapping.class);
      identityCheck.setAccessible(true);
      for (int i = 0; i < params.length; i++) {
        paramConverters[i] = mapping.mappingForType(params[i], mapping);
        if (skipParamsConversion) {
          skipParamsConversion = (Boolean)identityCheck.invoke(null,
              paramConverters[i]);
        }
      }
      // null converters denotes no conversion required for the case when
      // all parameter conversions are identity converters
      this.paramConverters = skipParamsConversion ? null : paramConverters;
      this.returnValConverter = mapping.mappingForType(
          m.getGenericReturnType(), mapping);
    } catch (OpenDataException ode) {
      throw new IllegalArgumentException("Failed getting converter of " +
          "parameter or return value to open type for given method: " + m, ode);
    } catch (Exception e) {
      throw new IllegalStateException("Exception getting converter of " +
          "parameter or return value to open type of given method: " + m, e);
    }
  }

  public Object[] convertParamsToOpenTypes(Object[] params)
      throws OpenDataException {
    final MXBeanMapping[] paramConverters = this.paramConverters;
    if (paramConverters != null && params != null && params.length > 0) {
      final Object[] openTypeParams = new Object[params.length];
      for (int i = 0; i < params.length; i++) {
        openTypeParams[i] = paramConverters[i].toOpenValue(params[i]);
      }
      return openTypeParams;
    } else {
      return params;
    }
  }

  public Object convertReturnValueToOpenType(Object returnVal)
      throws OpenDataException {
    return this.returnValConverter.toOpenValue(returnVal);
  }

  public Object convertOpenTypeToReturnValue(Object openVal)
      throws InvalidObjectException {
    return this.returnValConverter.fromOpenValue(openVal);
  }
}
