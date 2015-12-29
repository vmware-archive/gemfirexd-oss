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
 * GFXDDescriptor.cpp
 *
 *  Created on: Apr 17, 2013
 *      Author: shankarh
 */

#include "GFXDDescriptor.h"

using namespace com::pivotal::gemfirexd;

SQLRETURN GFXDDescriptor::newDescriptor(int descType,
    GFXDDescriptor*& descRef) {
  descRef = new GFXDDescriptor(descType);
  return SQL_SUCCESS;
}

SQLRETURN GFXDDescriptor::freeDescriptor(GFXDDescriptor* desc) {
  if (desc != NULL) {
    delete desc;
    desc = NULL;
  }
  return SQL_SUCCESS;
}

GFXDDescriptor::GFXDDescriptor(int descType) //:
//m_resultSetMetaData(NULL), m_paramMetaData(NULL)
    {
  m_descType = descType;
  //m_resultSetMetaData = NULL;
  //m_paramMetaData = NULL;
}

GFXDDescriptor::~GFXDDescriptor() {
}
