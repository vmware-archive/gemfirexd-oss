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
 * GFXDDefaults.cpp
 *
 *  Created on: Mar 21, 2013
 *      Author: shankarh
 */

#include "GFXDDefaults.h"

extern "C"
{
#include <string.h>
}

#define DEFAULT_DSN_NAME "Default"

static const SQLWCHAR* newSQLWCHAR(const char* str) {
  SQLWCHAR* wname = new SQLWCHAR[::strlen(str)];
  SQLWCHAR* pwname = wname;
  while ((*pwname++ = *str++) != 0)
    ;

  return wname;
}

bool GFXDGlobals::g_initialized = false;
bool GFXDGlobals::g_loggingEnabled = false;

const SQLCHAR GFXDDefaults::DEFAULT_DSN[] = { DEFAULT_DSN_NAME };
const SQLWCHAR* GFXDDefaults::DEFAULT_DSNW = newSQLWCHAR(DEFAULT_DSN_NAME);
const char* GFXDDefaults::ODBC_INI = "ODBC.INI";
const char* GFXDDefaults::ODBCINST_INI = "ODBCINST.INI";

// "localhost" is the default server
const char GFXDDefaults::DEFAULT_SERVER[] = { "localhost" };
// using the GFXD/Derby default client port
const int GFXDDefaults::DEFAULT_SERVER_PORT = 1527;

// define the last global exception (during initialization etc)
AutoPtr<SQLException> GFXDHandleBase::s_globalError;
