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

import java.util.Calendar;

public final class DateTimeUtils {

  public static Calendar copy(final Calendar dateTime) {
    if (dateTime != null) {
      final Calendar dateTimeCopy = Calendar.getInstance();
      dateTimeCopy.clear();
      dateTimeCopy.setTimeInMillis(dateTime.getTimeInMillis());
      return dateTimeCopy;
    }

    return null;
  }
  
  public static Calendar fromMilliseconds(final long ms) {
    final Calendar dateTime = Calendar.getInstance();
    dateTime.clear();
    dateTime.setTimeInMillis(ms);
    return dateTime;
  }
  
  public static long toMilliseconds(final Calendar dateTime) {
    return (dateTime == null ? 0 : dateTime.getTimeInMillis());
  }

}
