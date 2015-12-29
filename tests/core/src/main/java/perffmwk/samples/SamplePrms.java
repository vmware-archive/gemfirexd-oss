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

package perffmwk.samples;

import hydra.*;

/**
 *
 * A class used to store keys for sample performance test configuration settings.
 *
 */

public class SamplePrms extends BasePrms {

    /**
     *  (int)
     *  Number of warmup iterations.
     */
    public static Long warmupIterations;

    /**
     *  (int)
     *  Number of work iterations.
     */
    public static Long workIterations;

    static {
        setValues( SamplePrms.class );
    }

    public static void main( String args[] ) {
        Log.createLogWriter( "sampleprms", "info" );
        dumpKeys();
    }
}
