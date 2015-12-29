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

import static java.util.Arrays.asList;

import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.PathNotFoundException;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.internal.Utils;

import java.util.List;

/**
 *
 */
class PropertyPathToken extends PathToken {

    private final List<String> properties;

    public PropertyPathToken(List<String> properties) {
        this.properties = properties;
    }

    public List<String> getProperties() {
        return properties;
    }

    @Override
    void evaluate(String currentPath, Object model, EvaluationContextImpl ctx) {
//        if (!ctx.jsonProvider().isPdxInstance(model) && !ctx.jsonProvider().isContainer(model)) {
//            throw new PathNotFoundException("Property " + getPathFragment() + " not found in path " + currentPath);
//        }
//        handleObjectProperty(currentPath, model, ctx, properties);
        
      if (ctx.jsonProvider().isPdxInstance(model)) {
          handleObjectProperty(currentPath, model, ctx, properties);
      }
      else if (ctx.jsonProvider().isArray(model)) {
        for (int idx = 0; idx < ctx.jsonProvider().length(model); idx++) {
          Object evalHit = ctx.jsonProvider().getArrayIndex(model, idx);
          handleObjectProperty(currentPath, evalHit, ctx, properties);
        }
      }
      else {
        throw new PathNotFoundException("Property " + getPathFragment()
            + " not found in path " + currentPath);
      }
    }

    @Override
    boolean isTokenDefinite() {
        return true;
    }

    @Override
    public String getPathFragment() {
        return new StringBuilder()
                .append("[")
                .append(Utils.join(", ", "'", properties))
                .append("]").toString();
    }
}
