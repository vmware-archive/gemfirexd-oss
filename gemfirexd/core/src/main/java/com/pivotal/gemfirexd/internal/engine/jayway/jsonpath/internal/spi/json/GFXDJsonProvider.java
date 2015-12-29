/*
 * Copyright 2011 the original author or authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.internal.spi.json;

import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.InvalidJsonException;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.spi.json.Mode;

public class GFXDJsonProvider extends AbstractJsonProvider{

  @Override
  public Mode getMode() {
    return Mode.STRICT;
  }

  @Override
  public Object parse(String json) throws InvalidJsonException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object parse(Reader jsonReader) throws InvalidJsonException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object parse(InputStream jsonStream) throws InvalidJsonException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String toJson(Object obj) {
    return JSONFormatter.toJSON((PdxInstance)obj);
  }

  @Override
  public Map<String, Object> createMap() {
      return new LinkedHashMap<String, Object>();
  }

  @Override
  public List<Object> createArray() {
      return new LinkedList<Object>();
  }

  /**
   * Returns the keys from the given object or the indexes from an array
   *
   * @param obj an array or an object
   * @return the keys for an object or the indexes for an array
   */
  public Collection<String> getPropertyKeys(Object obj) {
      if (isArray(obj)) {
          List l = (List) obj;
          List<String> keys = new ArrayList<String>(l.size());
          for (int i = 0; i < l.size(); i++) {
              keys.add(String.valueOf(i));
          }
          return keys;
      } else {
          return ((PdxInstance)obj).getFieldNames();
      }
  }
}
