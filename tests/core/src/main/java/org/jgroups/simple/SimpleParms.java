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

package org.jgroups.simple;

import hydra.*;

/**
*
* A class used to store keys for distributed test configuration settings.
*
*/

public class SimpleParms extends BasePrms {

    public static Long blackboardName;
    public static Long blackboardType;
    
    public static Long sleepTime;
    public static Long duration;
    public static Long summary;
    public static Long task;
    
    public static Long jg1;
    public static Long jg2;
    public static Long jg3;
    public static Long jg4;
    public static Long jg5;
    public static Long jg6;
    public static Long jg7;
    public static Long jg8;
    public static Long jg9;
    public static Long jg10;
    public static Long jg11;
    
    static {
        setValues( SimpleParms.class );
    }
    public static void main( String args[] ) {
        dumpKeys();
    }
}
