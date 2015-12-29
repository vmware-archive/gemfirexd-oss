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
/**
 * 
 */
package diskRecovery;

import hydra.Log;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import util.TestException;
import util.TestHelper;

/** Class containing methods so the test can configure when certain classes
 *  (such as cache listeners and cache writers should be allowed to be 
 *  instantiated and when they should not. This is used by disk conversion tests
 *  to ensure that the disk conversion tool is not instantiating certain classes.
 * @author lynn
 *
 */
public class InstantiationHelper {

  public static final String wellKnownFileName = "allowInstaniation.ser";



  /** Return whether instantiation of certain classes is currently allowed or not. 
   * 
   * @return true if instantiation is allowed, false otherwise.
   */
  public static boolean allowInstantiation() {
    try {
      FileInputStream fis = new FileInputStream(wellKnownFileName);
      ObjectInputStream ois = new ObjectInputStream(fis);
      boolean allowInstantiation = ois.readBoolean();
      return allowInstantiation;
    } catch (FileNotFoundException e) {
      // if the file is not found, default to allow instantiation
      return true;
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Set whether instantiation of certain classes is allowed or not.
   * 
   * @param aVal true if instantiation is allowed, false otherwise. 
   */
  public static void setAllowInstantiation(boolean aVal) {
    try {
      FileOutputStream fos = new FileOutputStream(wellKnownFileName);
      ObjectOutputStream oos = new ObjectOutputStream(fos);
      oos.writeBoolean(aVal);
      oos.close();
      fos.close();
    } catch (FileNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    
    // verify the setting worked
    boolean allow = allowInstantiation();
    if (allow != aVal) {
      throw new TestException("Test problem; unable to retrieve allowInstantion from file");
    }
    Log.getLogWriter().info("Set allowInstantiation to " + aVal);
  }
}
