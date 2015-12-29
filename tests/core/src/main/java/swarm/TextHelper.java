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
package swarm;

public class TextHelper {

  
  public static String getShortName(String name) {
    String s = name.replaceAll("com\\.gemstone\\.gemfire\\.cache","c\\.g\\.g\\.c").replaceAll("com\\.gemstone\\.gemfire","c\\.g\\.g").replaceAll("\\.internal\\.","\\.in\\.").replaceAll("\\.cache\\.","\\.c\\.").replaceAll("\\.cache\\.","\\.c\\.").replaceAll("\\.tier\\.","\\.t\\.").replaceAll("\\.sockets\\.","\\.s\\.");
    if(s.length()>55) {
      s = s.substring(0,52)+"...";
    }
    return s;
  }

  public static String getVeryShortName(String name) {
    String s = name.replaceAll("com\\.gemstone\\.gemfire\\.","").replaceAll("cache\\.","").replaceAll(".*\\.","");
    if(s.length()>40) {
      s = s.substring(0,37)+"...";
    }
    return s;

  }

  
  public static String getShortMethodName(String s) {
    if(s.length()>40) {
      s = s.substring(0,37)+"...";
    }
    return s;
  }
  
  
}
