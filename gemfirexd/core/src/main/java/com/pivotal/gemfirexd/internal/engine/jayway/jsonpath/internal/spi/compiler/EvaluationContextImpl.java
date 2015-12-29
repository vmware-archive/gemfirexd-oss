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
package com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.internal.spi.compiler;

import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.Configuration;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.Option;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.PathNotFoundException;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.spi.compiler.EvaluationContext;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.spi.compiler.Path;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.spi.json.JsonProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
class EvaluationContextImpl implements EvaluationContext {

    private final Configuration configuration;
    private final Object valueResult;
    //private final Object pathResult;
    private final Path path;
    private int resultIndex = 0;

    EvaluationContextImpl(Path path, Configuration configuration) {
        this.path = path;
        this.configuration = configuration;
        this.valueResult = configuration.getProvider().createMap();
        //this.pathResult = configuration.getProvider().createArray();
    }

    void addResult(String path, Object model) {
        configuration.getProvider().setProperty(valueResult, resultIndex, model);
        //configuration.getProvider().setProperty(pathResult, resultIndex, path);
        resultIndex++;
    }

    public JsonProvider jsonProvider() {
        return configuration.getProvider();
    }

    public Set<Option> options() {
        return configuration.getOptions();
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }


    @Override
    public <T> T getValue() {
        if (path.isDefinite()) {
            if(resultIndex == 0){
                //throw new PathNotFoundException("No results for path: " + path.toString());
        // KISHOR: Do not throw PathNotFoundException, if the required attribued
        // did not find. just return null.
        // in multiple json strings in DB, its quite possible that particular
        // key is not added into json doc, in that case return nullfor the
        // missing key
              return null;
            }
            //return (T) jsonProvider().getArrayIndex(valueResult, 0);
            return (T) jsonProvider().getProperty(valueResult, 0);
        }
        return (T) valueResult;
    }

    @Override
    public <T> T getPath() {
        if(resultIndex == 0){
            throw new PathNotFoundException("No results for path: " + path.toString());
        }
        //return (T)pathResult;
        return null;
    }

    @Override
    public List<String> getPathList() {
//        List<String> res = new ArrayList<String>();
//        if(resultIndex > 0){
//            Iterable<Object> objects = configuration.getProvider().toIterable(pathResult);
//            for (Object o : objects) {
//                res.add((String)o);
//            }
//        }
//        return res;
      return null;
    }

}
