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
package query.common;

public class QueryValidator implements java.io.Serializable {
    private String operationDone = null;
    private Object key = "";
    private boolean keyExists = false;
    private boolean hasValue = false;
    private Object expectedValue = null;
    public QueryValidator(String operation, Object k, boolean exists, boolean hasVal, Object expectedVal) {
       this.operationDone = operation;
       this.key = k;
       this.keyExists = exists;
       this.hasValue = hasVal;
       this.expectedValue = expectedVal;
    }
    public QueryValidator() {
         
    }
    public String getOperation() {
        return this.operationDone;
    }
    
    public Object getKey() {
        return this.key;
    }
    
    public boolean getKeyExists() {
        return this.keyExists;
    }
    
    public boolean getHasValue() {
        return this.hasValue;
    }
    
    public Object getValue() {
        return this.expectedValue;
    }
    /**
     * @param object
     */
    public void setValue(Object anObj) {
      expectedValue = anObj;
    }
}
