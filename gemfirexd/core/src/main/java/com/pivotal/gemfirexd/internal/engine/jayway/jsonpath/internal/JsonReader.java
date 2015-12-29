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
package com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.internal;

import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.Configuration;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.Filter;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.JsonPath;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.ParseContext;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.ReadContext;
import com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.spi.http.HttpProviderFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import static com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.internal.Utils.notEmpty;
import static com.pivotal.gemfirexd.internal.engine.jayway.jsonpath.internal.Utils.notNull;

public class JsonReader implements ParseContext, ReadContext {

    private final Configuration configuration;
    private Object json;

    public JsonReader() {
        this(Configuration.defaultConfiguration());
    }

    public JsonReader(Configuration configuration) {
        notNull(configuration, "configuration can not be null");
        this.configuration = configuration;
    }

    //------------------------------------------------
    //
    // ParseContext impl
    //
    //------------------------------------------------
    @Override
    public ReadContext parse(Object json) {
        notNull(json, "json object can not be null");
        this.json = json;
        return this;
    }

    @Override
    public ReadContext parse(String json) {
        notEmpty(json, "json string can not be null or empty");
        this.json = configuration.getProvider().parse(json);
        return this;
    }

    @Override
    public ReadContext parse(InputStream json) {
        notNull(json, "json input stream can not be null");
        this.json = configuration.getProvider().parse(json);
        return this;
    }

    @Override
    public ReadContext parse(File json) throws IOException {
        notNull(json, "json file can not be null");
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(json);
            parse(fis);
        } finally {
            Utils.closeQuietly(fis);
        }
        return this;
    }

    @Override
    public ReadContext parse(URL json) throws IOException {
        notNull(json, "json url can not be null");
        InputStream is = HttpProviderFactory.getProvider().get(json);
        return parse(is);
    }

    //------------------------------------------------
    //
    // ReadContext impl
    //
    //------------------------------------------------
    @Override
    public Object json() {
        return json;
    }

    @Override
    public <T> T read(String path, Filter... filters) {
        notEmpty(path, "path can not be null or empty");
        return read(JsonPath.compile(path, filters));
    }

    @Override
    public <T> T read(JsonPath path) {
        notNull(path, "path can not be null");
        return path.read(json, configuration);
    }

}
