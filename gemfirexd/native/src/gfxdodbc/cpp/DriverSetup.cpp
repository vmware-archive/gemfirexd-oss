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
 * DriverSetup.cpp
 *
 * This file currently contains the ODBCINSTGetProperties function required by
 * unixODBC (http://www.unixodbc.org) to define DSNs.
 *
 *      Author: swale
 */

#include "GFXDEnvironment.h"
#include "OdbcIniKeys.h"

#ifndef _WINDOWS

extern "C"
{
#include <odbcinstext.h>
#include <stdlib.h>
#include <string.h>
}

using namespace com::pivotal::gemfirexd;

int ODBCINSTGetProperties(HODBCINSTPROPERTY lastProperty) {
  // loop through all the properties
  const OdbcIniKeys::KeyMap& keyMap = OdbcIniKeys::getKeyMap();
  for (OdbcIniKeys::KeyMap::const_iterator iter = keyMap.begin();
      iter != keyMap.end(); ++iter) {
    const std::string& propName = iter->first;
    const std::string& helpMessage = iter->second->getHelpMessage();
    const int flags = iter->second->getFlags();
    const std::vector<std::string>* values = NULL;

    // zero size help message indicates don't display the property
    // (either repeat or not yet implemented/supported)
    if (helpMessage.size() == 0) {
      continue;
    }

    lastProperty->pNext = (HODBCINSTPROPERTY)::malloc(sizeof(ODBCINSTPROPERTY));
    lastProperty = lastProperty->pNext;
    ::memset(lastProperty, 0, sizeof(ODBCINSTPROPERTY));
    if ((flags & (ConnectionProperty::F_IS_PASSWD
        | ConnectionProperty::F_IS_PASSWD_FIELD)) != 0) {
      lastProperty->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT_PASSWORD;
    } else if ((flags & ConnectionProperty::F_IS_FILENAME) != 0) {
      lastProperty->nPromptType = ODBCINST_PROMPTTYPE_FILENAME;
    } else if ((values = iter->second->getPossibleValues()) != NULL) {
      lastProperty->nPromptType = ODBCINST_PROMPTTYPE_LISTBOX;
    } else {
      lastProperty->nPromptType = ODBCINST_PROMPTTYPE_TEXTEDIT;
    }

    ::strncpy(lastProperty->szName, propName.c_str(), propName.size() + 1);
    lastProperty->pszHelp = (char*)::malloc(helpMessage.size() + 1);
    ::strncpy(lastProperty->pszHelp, helpMessage.c_str(),
        helpMessage.size() + 1);

    int numValues;
    if (values == NULL || (numValues = values->size()) == 0) {
      ::strncpy(lastProperty->szValue, "", 1);
    } else {
      lastProperty->aPromptData = (char**)::malloc(sizeof(char*) * numValues);
      ::memcpy(lastProperty->aPromptData, values, sizeof(char*) * numValues);
      ::strncpy(lastProperty->szValue, values->operator [](0).c_str(),
      INI_MAX_PROPERTY_VALUE);
    }
  }

  return 1;
}

#endif
