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

#ifndef _CONFIGSETTINGS_H_
#define _CONFIGSETTINGS_H_


//#include "stdafx.h"
#ifdef _WIN32
#include <windows.h>
#endif
#include <map>
#include <vector>
#include <string>


/// Odbc.ini subkeys.
/// NOTE: These keys must be inserted into the vector of configuration keys during construction,
///       so add new configuration keys to the vector via InitializeConfigurationKeys routine.
#define SETTING_DRIVER          "Driver"
#define SETTING_DSN             "DSN"
#define SETTING_DESCRIPTION     "DESCRIPTION"
#define SETTING_SERVER					"SERVER"
#define SETTING_PORT				    "PORT"
#define SETTING_UID							"UID"
#define SETTING_PWD							"PWD"
#define SETTING_SETUP           "Setup"


typedef size_t                      gemfirexd_handle;
typedef size_t                      gemfirexd_size_t;

/// Configuration Map
typedef std::map<std::string, std::string> ConfigurationMap;

/// @brief This class encapsulates the settings for the configuration dialog.
class ConfigSettings
{
// Public ======================================================================================
public:
    /// @brief Constructor.
    ConfigSettings();

    /// @brief Destructor.
    ///
    /// Release locally held resources.
    ~ConfigSettings();

    /// @brief Get the DSN name.
    ///
    /// @return DSN name configured.
    const std::string& GetDSN();

    /// @brief Get the description.
    ///
    /// @return Description configured.
    const std::string& GetDescription();

		/// @brief Get the server.
		/// 
		/// @return Server configured.
		const std::string& GetSERVER();

		/// @brief Get the port.
		/// 
		/// @return Server configured.
		const std::string& GetPORT();

		/// @return Password configured.
		const std::string& GetPWD();

		/// @brief Get the user name.
		/// 
		/// @return User name configured.
		const std::string& GetUID();

    /// @brief Set the DSN name.
    ///
    /// @param in_dsn                       DSN name to configure.
    ///
    /// @exception ErrorException
    void SetDSN(const std::string& in_dsn);

    /// @brief Set the description.
    ///
    /// @param in_description               Description to configure.
    void SetDescription(const std::string& in_description);

		/// @brief Set the PWD.
		/// 
		/// @param in_pwd						Password to configure.
		void SetPWD(const std::string& in_pwd);

		/// @brief Set the SERVER.
		/// 
		/// @param in_server						The user id to configure.
		void SetSERVER(const std::string& in_server);

		/// @brief Set the PORT.
		/// 
		/// @param in_port						The user id to configure.
		void SetPORT(const std::string& in_port);

		/// @param in_uid						The user id to configure.
		void SetUID(const std::string& in_uid);

    /// @brief Updates the member variables and configuration map with odbc.ini configurations.
    ///
    /// The configuration map will contain the delta, whereas the custom configuration map 
    /// will be completely overwritten.
    void ReadConfigurationDriver(const char* fileName);

    /// @brief Updates the member variables and configuration map with odbc.ini configurations.
    ///
    /// The configuration map will contain the delta, whereas the custom configuration map 
    /// will be completely overwritten.
    void ReadConfigurationDSN();

    /// @brief Updates the odbc.ini configurations from the configuration map.
    void WriteConfiguration(const char* fileName);

    /// @brief Clears current configuration, resets all private data members to their default
    /// values, then updates the configuration map with those configurations specified in the
    /// in_dsnConfigurationMap.
    ///
    /// (No implicit update to odbc.ini)
    ///
    /// @param in_dsnConfigurationMap       This will at least hold the DSN entry, which can be 
    ///                                     used to load the other settings.
    void UpdateConfiguration(ConfigurationMap& in_dsnConfigurationMap);

// Private =====================================================================================
private:
    /// Configuration map of odbc.ini subkeys and values
    ConfigurationMap m_configuration;

    /// The driver for this DSN.
    std::string m_driver;

    /// The DSN name.
    std::string m_dsn;

    /// The original DSN name; used to determine if the DSN name has changed.
    std::string m_dsnOrig;

    /// @brief Reads all the settings for the given section
    ///
    /// @param sectionName                  The DSN or Driver section name
    /// @param filename                     The filename to read from, should be odbc.ini or odbcinst.ini
    ///
    /// @return A map of all settings in the section
    ConfigurationMap ReadSettings(std::string sectionName, std::string filename);
};


#endif
