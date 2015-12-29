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
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.Filter;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.InvalidPathException;

import java.util.Collection;

import static java.lang.String.format;
import static java.util.Arrays.asList;

/**
 *
 */
class FilterPathToken extends PathToken {

    private static final String[] FRAGMENTS = {
            "[?]",
            "[?,?]",
            "[?,?,?]",
            "[?,?,?,?]",
            "[?,?,?,?,?]"
    };

    private final Collection<Filter> filters;

    public FilterPathToken(Filter filter) {
        this.filters = asList(filter);
    }

    public FilterPathToken(Collection<Filter> filters) {
        this.filters = filters;
    }

    @Override
    void evaluate(String currentPath, Object model, EvaluationContextImpl ctx) {
        if (!ctx.jsonProvider().isArray(model)) {
            throw new InvalidPathException(format("Filter: %s can only be applied to arrays. Current context is: %s", toString(), model));
        }
        int idx = 0;
        Iterable<Object> objects = ctx.jsonProvider().toIterable(model);

        for (Object idxModel : objects) {
            if (accept(idxModel, ctx.configuration())) {
                handleArrayIndex(idx, currentPath, model, ctx);
            }
            idx++;
        }
    }

    public boolean accept(Object obj, Configuration configuration) {
        boolean accept = true;

        for (Filter filter : filters) {
            if (!filter.apply (obj, configuration)) {
                accept = false;
                break;
            }
        }

        return accept;
    }

    @Override
    public String getPathFragment() {
        return FRAGMENTS[filters.size() - 1];
    }

    @Override
    boolean isTokenDefinite() {
        return false;
    }
}
