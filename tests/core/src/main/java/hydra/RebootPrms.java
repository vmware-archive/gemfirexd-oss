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

import hydra.BasePrms;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class RebootPrms extends BasePrms {

  static {
    setValues(RebootPrms.class); // initialize parameters
  }

  private static Map<String,String> ConfigurationFileNames;
  private static Map<String,String> BaseHosts;

  /**
   * (boolean)
   * Whether to use a hard stop when rebooting. Defaults to true.
   */
  public static Long useHardStop;

  public static boolean useHardStop() {
    Long key = useHardStop;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (String)
   * Name of the file containing a map of each virtual machine name to the
   * host on which it runs and the path to its configuration file. This is a
   * required parameter.
   * <p>
   * For example, this line indicates the virtual machine w2-gst-cert-22 runs
   * on w2-gst-dev41 using the given configuration file:
   * <code>
   * w2-gst-cert-22 w2-gst-dev41 /w2-gst-dev41a/vms/w2-gst-cert-22/w2-gst-cert-22.vmx
   * </code>
   */
  public static Long vmMapFileName;

  public static String getVMMapFileName() {
    Long key = vmMapFileName;
    String fn = tasktab().pathAt(key, tab().stringAt(key, null));
    if (fn == null) {
      String s = BasePrms.nameForKey(key) + " is a required parameter";
      throw new HydraConfigException(s);
    }
    return fn;
  }

  private static List<String> getVMMapFileContents() {
    String fn = getVMMapFileName();
    try {
      return FileUtil.getTextAsList(fn);
    } catch (FileNotFoundException e) {
      String s = BasePrms.nameForKey(vmMapFileName) + " cannot be loaded: "
               + fn;
      throw new HydraConfigException(s, e);
    } catch (IOException e) {
      String s = BasePrms.nameForKey(vmMapFileName) + " cannot be loaded: "
               + fn;
      throw new HydraConfigException(s, e);
    }
  }

  public static synchronized String getBaseHost(String vm) {
    if (BaseHosts == null) {
      BaseHosts = new HashMap<String,String>();
      List<String> lines = getVMMapFileContents();
      for (String line : lines) {
        String[] tokens = line.split(" ");
        if (tokens.length != 3) {
          String s = BasePrms.nameForKey(vmMapFileName)
                   + " has a malformed line: " + line;
          throw new HydraConfigException(s);
        }
        BaseHosts.put(tokens[0], tokens[1]);
      }
    }
    String baseHost = BaseHosts.get(vm);
    if (baseHost == null) {
      String s = BasePrms.nameForKey(vmMapFileName) + " does not contain " + vm;
      throw new HydraConfigException(s);
    }
    return baseHost;
  }

  public static synchronized String getConfigurationFileName(String vm) {
    if (ConfigurationFileNames == null) {
      ConfigurationFileNames = new HashMap<String,String>();
      List<String> lines = getVMMapFileContents();
      for (String line : lines) {
        String[] tokens = line.split(" ");
        if (tokens.length != 3) {
          String s = BasePrms.nameForKey(vmMapFileName)
                   + " has a malformed line: " + line;
          throw new HydraConfigException(s);
        }
        ConfigurationFileNames.put(tokens[0], tokens[2]);
      }
    }
    return ConfigurationFileNames.get(vm);
  }
}
