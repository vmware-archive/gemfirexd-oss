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
/*
 * Street.java
 *
 * Created on September 30, 2005, 1:26 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author vjadhav
 */
public class Street {
    public String street;
    public String lane;
    /** Creates a new instance of Street */
    public Street(String street,String lane) {
        this.street = street;
        this.lane = lane;
    }
    
}