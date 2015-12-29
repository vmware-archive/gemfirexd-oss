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
package container.app.util;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.text.MessageFormat;

public final class IOUtils {

  public static InputStream open(final URL url) {
    try {
      return url.openStream();
    }
    catch (IOException e) {
      throw new RuntimeException(MessageFormat.format("Failed to open an input stream to resource ({0})!", url), e);
    }
  }

  public static void close(final Closeable obj) {
    close(obj, true);
  }

  public static void close(final Closeable obj, final boolean silent) {
    try {
      obj.close();
    } 
    catch (IOException e) {
      if (silent) {
        e.printStackTrace();
      }
      else {
        throw new RuntimeException(MessageFormat.format("Failed to close resource ({0})!", obj), e);
      }
    }
  }

}
