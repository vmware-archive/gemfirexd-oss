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

package hydra;

/**
 *
 * A class used to store keys for Dunit's JUnitTestTask configuration settings.
 * <p>
 * The remaining parameters
 * have the indicated default values.  If fewer non-default values than names
 * are given for these parameters, the remaining instances will use the last
 * value in the list.  See $JTESTS/hydra/hydra.txt for more details.
 *
 */

public class JUnitTestTaskPrms extends BasePrms {

    /**
     *  (boolean)
     *  Control if JUnitTestTask should create junit formatters.
     */
    public static Long useJUnitFormatter;

    static {
        setValues( JUnitTestTaskPrms.class );
    }

    public static void main( String args[] ) {
        Log.createLogWriter( "JUnitTestTaskPrms", "info" );
        dumpKeys();
    }
}
