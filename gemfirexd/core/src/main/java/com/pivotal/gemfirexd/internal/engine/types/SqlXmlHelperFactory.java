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

package com.pivotal.gemfirexd.internal.engine.types;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;

/**
 * Factory for {@link SqlXmlHelper}. Uses reflection to generate the actual
 * implementation object.
 * 
 * @author swale
 */
public final class SqlXmlHelperFactory {

  private static final boolean useSun5 = sun5XalanClasses();

  public static final SqlXmlHelper newInstance() throws StandardException {
    try {
      if (useSun5) {
        return (SqlXmlHelper)Class.forName(
            "com.pivotal.gemfirexd.internal.engine.types.SqlXmlHelperSun5")
            .newInstance();
      }
      else {
        return (SqlXmlHelper)Class.forName(
            "com.pivotal.gemfirexd.internal.engine.types.SqlXmlHelperXalan")
            .newInstance();
      }
    } catch (InstantiationException ie) {
      throw StandardException.newException(
          SQLState.LANG_UNEXPECTED_XML_EXCEPTION, ie, ie.getLocalizedMessage());
    } catch (IllegalAccessException iae) {
      throw StandardException.newException(
          SQLState.LANG_UNEXPECTED_XML_EXCEPTION, iae,
          iae.getLocalizedMessage());
    } catch (ClassNotFoundException cnfe) {
      throw StandardException.newException(SQLState.LANG_MISSING_XML_CLASSES,
          "SQLXMLHELPER");
    } catch (LinkageError err) {
      throw StandardException.newException(SQLState.LANG_MISSING_XML_CLASSES,
          err, "XALAN");
    }
  }

  public static final boolean sun5XalanClasses() {
    // try loading the Sun5 classes
    try {
      Class.forName("com.sun.org.apache.xml.internal.serializer.DOMSerializer");
    } catch (ClassNotFoundException cnfe) {
      return false;
    }
    return true;
  }
}
