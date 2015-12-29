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

import com.gemstone.gemfire.pdx.PdxInstance;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.InvalidPathException;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.Option;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.PathNotFoundException;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.internal.Utils;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.spi.json.JsonProvider;

import java.util.List;

abstract class PathToken {

    private PathToken next;
    private Boolean definite = null;

    PathToken appendTailToken(PathToken next) {
        this.next = next;
        return next;
    }

    void handleObjectProperty(String currentPath, Object model, EvaluationContextImpl ctx, List<String> properties) {

        if(properties.size() == 1) {
            String property = properties.get(0);
            String evalPath = currentPath + "['" + property + "']";
            Object propertyVal = readObjectProperty(property, model, ctx);
            if(propertyVal == JsonProvider.UNDEFINED){
                return;
            }
            if (isLeaf()) {
                ctx.addResult(evalPath, propertyVal);
            } else if ((next().isLeaf()) && (next() instanceof WildcardPathToken)){
              ctx.addResult(evalPath, propertyVal);
              return;
            }
            else {
                next().evaluate(evalPath, propertyVal, ctx);
            }
        } else { // TODO : KISHOR : This else loop is not tested yet
            String evalPath = currentPath + "[" + Utils.join(", ", "'", properties) + "]";

            if (!isLeaf()) {
                throw new InvalidPathException("Multi properties can only be used as path leafs: " + evalPath);
            }

            if(ctx.configuration().containsOption(Option.MERGE_MULTI_PROPS)) {
                Object map = ctx.jsonProvider().createMap();
                for (String property : properties) {
                    Object propertyVal = readObjectProperty(property, model, ctx);
                    if(propertyVal == JsonProvider.UNDEFINED){
                        continue;
                    }
                    ctx.jsonProvider().setProperty(map, property, propertyVal);
                }
                ctx.addResult(evalPath, map);

            } else {
                for (String property : properties) {
                    evalPath = currentPath + "['" + property + "']";
                    if(hasProperty(property, model, ctx)) {
                        Object propertyVal = readObjectProperty(property, model, ctx);
                        if(propertyVal == JsonProvider.UNDEFINED){
                            continue;
                        }
                        ctx.addResult(evalPath, propertyVal);
                    }
                }
            }
        }
    }

    private boolean hasProperty(String property, Object model, EvaluationContextImpl ctx) {
        return ctx.jsonProvider().getPropertyKeys(model).contains(property);
    }

    private Object readObjectProperty(String property, Object model, EvaluationContextImpl ctx) {
      if (model instanceof PdxInstance) {
        Object val = ctx.jsonProvider().getPDXValue(model, property, true);
        if (val == JsonProvider.UNDEFINED) {
          if (ctx.options().contains(Option.THROW_ON_MISSING_PROPERTY)) {
            throw new PathNotFoundException("Property ['" + property
                + "'] not found in the current context");
          }
        }
        return val;
      }
      return model;
    }

    void handleArrayIndex(int index, String currentPath, Object json, EvaluationContextImpl ctx) {
        String evalPath = currentPath + "[" + index + "]";
        try {
            Object evalHit = ctx.jsonProvider().getArrayIndex(json, index);
            if (isLeaf()) {
                ctx.addResult(evalPath, evalHit);
            } else {
                next().evaluate(evalPath, evalHit, ctx);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new PathNotFoundException("Index out of bounds when evaluating path " + currentPath + "[" + index + "]");
        }
    }


    PathToken next() {
        if (isLeaf()) {
            throw new IllegalStateException("Current path token is a leaf");
        }
        return next;
    }

    boolean isLeaf() {
        return next == null;
    }


    public int getTokenCount() {
        int cnt = 1;
        PathToken token = this;

        while (!token.isLeaf()){
            token = token.next();
            cnt++;
        }
        return cnt;
    }

    public boolean isPathDefinite() {
        if(definite != null){
            return definite.booleanValue();
        }
        boolean isDefinite = isTokenDefinite();
        if (isDefinite && !isLeaf()) {
            isDefinite = next.isPathDefinite();
        }
        definite = isDefinite;
        return isDefinite;
    }

    @Override
    public String toString() {
        if (isLeaf()) {
            return getPathFragment();
        } else {
            return getPathFragment() + next().toString();
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    abstract void evaluate(String currentPath, Object model, EvaluationContextImpl ctx);

    abstract boolean isTokenDefinite();

    abstract String getPathFragment();

}
