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

import java.text.SimpleDateFormat;
import java.util.Calendar;

public final class DateTimeFormatUtils {
  
  public static final String DEFAULT_DATE_FORMAT_PATTERN = "MM/dd/yyyy hh:mm a";
  
  public static String format(final Calendar dateTime) {
    return format(dateTime, DEFAULT_DATE_FORMAT_PATTERN);
  }
  
  public static String format(final Calendar dateTime, String dateFormatPattern) {
    if (dateTime != null) {
      dateFormatPattern = ObjectUtils.defaultIfNull(dateFormatPattern, DEFAULT_DATE_FORMAT_PATTERN);
      return new SimpleDateFormat(dateFormatPattern).format(dateTime.getTime());
    }

    return null;
  }

}
