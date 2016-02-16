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
 * Numbers.java
 *
 * Created on November 9, 2005, 12:13 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author  prafulla
 */
import java.io.Serializable;

public class Numbers implements Serializable{
    ///////fields of class
    public int id;
    public int id1;
    public int id2;
    public float avg1;
    public float max1;
    public double range;
    public long l;
    
    /** Creates a new instance of Numbers */
    public Numbers(int i) {
        id = i;
        id1 = -1*id;
        id2 = 1000 - id;
        avg1 = (id + id1 + id2) / 3;
        max1 = id;
        range = (id - id1);
        l = id*100000000;
    }
    
}
