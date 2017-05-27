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
 * PhoneNo.java
 *
 * Created on September 29, 2005, 3:40 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author vjadhav
 */
public class PhoneNo {
    public int phoneNo1;
    public int phoneNo2;
    public int phoneNo3;
    public int mobile;
    /** Creates a new instance of PhoneNo */
    public PhoneNo(int i,int j, int k, int m) {
        this.phoneNo1 = i;
        this.phoneNo2 = j;
        this.phoneNo3 = k;
        this.mobile = m;
    }
    
}//end of class
