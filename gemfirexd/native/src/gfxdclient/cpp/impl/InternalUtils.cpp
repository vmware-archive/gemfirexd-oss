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
 *  Created on: 7 Jun 2014
 *      Author: swale
 */

#include "InternalUtils.h"

extern "C"
{
#include <time.h>
}

using namespace com::pivotal::gemfirexd::client::impl;

const char InternalUtils::s_hexDigits[] = "0123456789abcdef";

boost::posix_time::ptime InternalUtils::s_epoch;
boost::local_time::time_zone_ptr InternalUtils::s_localTimeZone;
std::string InternalUtils::s_localTimeZoneStr;
bool InternalUtils::s_initialized = InternalUtils::staticInitialize();

bool InternalUtils::staticInitialize() {
  s_epoch = boost::posix_time::ptime(boost::gregorian::date(1970, 1, 1));

  // get the local timezone
  time_t ts = 0;
  struct tm t;
  char buf[16], bufA[16];
  std::string bufStr;
  ::localtime_r(&ts, &t);

  bool addColon = false;
  size_t buflen = ::strftime(buf, sizeof(buf), "%z", &t);
  if (buflen == 5 && (buf[3] != ':' && buf[3] != '-')) {
    addColon = true;
  }
  buflen = ::strftime(bufA, sizeof(bufA), "%Z", &t);
  bufStr.assign(bufA, buflen);
  bufStr.append(buf);
  if (addColon) {
    bufStr.insert(bufStr.length() - 2, ":");
  }

  s_localTimeZone = boost::local_time::time_zone_ptr(
      new boost::local_time::posix_time_zone(bufStr));
  if (s_localTimeZone->std_zone_abbrev().length() > 0) {
    s_localTimeZoneStr = s_localTimeZone->std_zone_abbrev();
  }
  else if (s_localTimeZone->std_zone_name().length() > 0) {
    s_localTimeZoneStr = s_localTimeZone->std_zone_name();
  }
  else {
    s_localTimeZoneStr = s_localTimeZone->to_posix_string();
  }
  return true;
}

boost::filesystem::path InternalUtils::getPath(const std::string& pathStr) {
  // Locale brain-dead Windows. It does not support UTF8 encodings rather
  // provides an "open" which accepts UTF16 filename. So we need to
  // convert to wchar_t here on Windows (at least if filename is not ASCII).
#ifdef _WINDOWS
  std::wstring wlogFile;
  if (Utils::convertUTF8ToUTF16(logFile.c_str(), logFile.size(), wlogFile)) {
    return boost::filesystem::path(wlogFile.begin(), wlogFile.end());
  }
#endif
  return boost::filesystem::path(pathStr);
}
