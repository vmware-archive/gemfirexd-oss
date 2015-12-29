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
   
package tx;

//import com.gemstone.gemfire.LogWriter;
//import hydra.*;
//import java.io.*;
//import util.*;

/**
 * A version of <code>ViewTest</code> that performs operations 
 * serially.  It validates the applications view of transaction 
 * vs. committed state.
 */

public class SerialViewTest extends ViewTest {

  public synchronized static void HydraTask_initialize() {
    if (viewTest == null) {
      viewTest = new SerialViewTest();
      viewTest.initialize();
    }
  }
}
