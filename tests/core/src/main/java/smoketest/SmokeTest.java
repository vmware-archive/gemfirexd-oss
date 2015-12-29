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

package smoketest;

import batterytest.BatteryTest;
import hydra.FileUtil;
import java.io.*;
import junit.framework.*;

public class SmokeTest extends TestCase {

  public SmokeTest( String name ) {
    super( name );
  }
  public void testSmoke() {
    if ( ! BatteryTest.runbattery(new String[0]) ) {
      String testFileName = System.getProperty( "testFileName" );
      String resultDir = System.getProperty( "resultDir" );
      String onelinerTxt = resultDir + File.separator + "oneliner.txt";
      String batterytestLog = resultDir + File.separator + "batterytest.log";
      String results = null;
      try {
	results = FileUtil.getText( onelinerTxt );
      } catch( FileNotFoundException e ) {
        results = onelinerTxt + " not found";
      } catch( IOException e ) {
        e.printStackTrace();
        results = onelinerTxt + " cannot be accessed";
      }
      fail
      (
        "One or more tests in " + testFileName + " failed.  " +
	"See " + batterytestLog + " and individual test directories for details." +
	"\n\nSUMMARY OF TEST RESULTS:\n\n" + results
      );
    }
  }
}
