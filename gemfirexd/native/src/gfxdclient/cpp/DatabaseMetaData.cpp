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
 * DatabaseMetaData.cpp
 *
 *      Author: swale
 */

#include "DatabaseMetaData.h"

#include <set>

using namespace com::pivotal::gemfirexd::client;

/** default constructor */
DatabaseMetaDataArgs::DatabaseMetaDataArgs() :
    m_args()
{
}

/** copy constructor */
DatabaseMetaDataArgs::DatabaseMetaDataArgs(const DatabaseMetaDataArgs& other) :
    m_args(other.m_args)
{
}

/** assignment operator */
DatabaseMetaDataArgs& DatabaseMetaDataArgs::operator=(
    const DatabaseMetaDataArgs& other) throw ()
{
  m_args = other.m_args;
  return *this;
}

DatabaseMetaDataArgs::~DatabaseMetaDataArgs() throw ()
{
}

DatabaseMetaData::DatabaseMetaData() :
    m_metadata()
{
}

DatabaseMetaData::DatabaseMetaData(const DatabaseMetaData& other) :
    m_metadata(other.m_metadata)
{
}

DatabaseMetaData& DatabaseMetaData::operator=(
    const DatabaseMetaData& other) throw ()
{
  m_metadata = other.m_metadata;
  return *this;
}

bool DatabaseMetaData::isFeatureSupported(
    DatabaseFeature::type feature) const throw ()
{
  const std::set<DatabaseFeature::type>& supportedFeatures =
      m_metadata.supportedFeatures;

  return supportedFeatures.find(feature) != supportedFeatures.end();
}

bool DatabaseMetaData::supportsConvert(SQLType::type fromType,
    SQLType::type toType) const throw ()
{
  std::map<thrift::GFXDType::type, std::set<thrift::GFXDType::type> >
      ::const_iterator result = m_metadata.supportedCONVERT.find(fromType);
  if (result != m_metadata.supportedCONVERT.end()) {
    return result->second.find(toType) != result->second.end();
  }
  else {
    return false;
  }
}

bool DatabaseMetaData::supportsTransactionIsolationLevel(
    IsolationLevel::type isolation) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::TRANSACTIONS_SUPPORT_ISOLATION,
      isolation);
}

bool DatabaseMetaData::supportsResultSetReadOnly(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_CONCURRENCY_READ_ONLY,
      rsType);
}

bool DatabaseMetaData::supportsResultSetUpdatable(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_CONCURRENCY_UPDATABLE,
      rsType);
}

bool DatabaseMetaData::othersUpdatesVisible(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_OTHERS_UPDATES_VISIBLE,
      rsType);
}

bool DatabaseMetaData::othersDeletesVisible(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_OTHERS_DELETES_VISIBLE,
      rsType);
}

bool DatabaseMetaData::othersInsertsVisible(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_OTHERS_INSERTS_VISIBLE,
      rsType);
}

bool DatabaseMetaData::ownUpdatesVisible(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_OWN_UPDATES_VISIBLE,
      rsType);
}

bool DatabaseMetaData::ownDeletesVisible(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_OWN_DELETES_VISIBLE,
      rsType);
}

bool DatabaseMetaData::ownInsertsVisible(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_OWN_INSERTS_VISIBLE,
      rsType);
}

bool DatabaseMetaData::updatesDetected(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_UPDATES_DETECTED, rsType);
}

bool DatabaseMetaData::deletesDetected(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_DELETES_DETECTED, rsType);
}

bool DatabaseMetaData::insertsDetected(
    ResultSetType::type rsType) const throw ()
{
  return searchFeature(
      thrift::ServiceFeatureParameterized::RESULTSET_INSERTS_DETECTED, rsType);
}

bool DatabaseMetaData::searchFeature(
    thrift::ServiceFeatureParameterized::type featureName,
    int32_t searchFor) const throw ()
{
  std::map<thrift::ServiceFeatureParameterized::type, std::vector<int32_t> >
      ::const_iterator result = m_metadata.featuresWithParams.find(featureName);
  if (result != m_metadata.featuresWithParams.end()) {
    for (std::vector<int32_t>::const_iterator iter = result->second.begin();
        iter != result->second.end(); ++iter) {
      if (*iter == searchFor) {
        return true;
      }
    }
  }
  return false;
}

DatabaseMetaData::~DatabaseMetaData() throw ()
{
}
