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
package s2qa;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;

public class TracQuery {
  public static final String DEFAULT_TRAC_REPORT_NUM = "118";
  public static final String PROP_TRAC_USER = "trac.user";
  public static final String PROP_TRAC_PASSWORD = "trac.password";
  public static final String PROP_TRAC_URL = "trac.url";
  public static final String PROP_TRAC_REPORT_NUM = "trac.reportNum";
  public static String DEFAULT_TRAC_URL="https://svn.gemstone.com/trac/gemfire/report/"+
    System.getProperty(PROP_TRAC_REPORT_NUM,DEFAULT_TRAC_REPORT_NUM)+
    "?format=csv&sort=ticket&asc=1&USER="+System.getProperty(PROP_TRAC_USER,System.getProperty("user.name"));
  public TracQuery()  {
  }

  public String runQuery() throws MalformedURLException, IOException {
    URL url = new URL(System.getProperty(PROP_TRAC_URL, DEFAULT_TRAC_URL));
    InputStream is = url.openStream();
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    StringBuffer sb = new StringBuffer();
    while ((line = br.readLine()) != null) {
      sb.append(line);
    }
    return sb.toString();
}

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      TracQuery tracQuery = new TracQuery();
      String results = tracQuery.runQuery();
      System.out.println(results);
      if (args.length == 2) {
        if (args[0].equals("-o")) {
          File f = new File(args[1]);
          PrintWriter pw = new PrintWriter(new FileWriter(f));
          pw.print(results);
          pw.close();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

  }

}
