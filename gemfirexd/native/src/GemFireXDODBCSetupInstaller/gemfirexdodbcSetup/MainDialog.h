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

#ifndef _MAINDIALOG_H_
#define _MAINDIALOG_H_


#include "stdafx.h"
#include "ConfigSettings.h"


/// @brief This class implements the main dialog for the DSN configuration.
class MainDialog
{
// Public ======================================================================================
public:
    /// @brief Constructor.
    MainDialog();

    /// @brief This function is passed to the Win32 dialog functions as a callback, and will 
    /// call out to the appropriate Do*Action functions.
    ///
    /// @param hwndDlg              The window handle of the dialog.
    /// @param message              Used to control the window action.
    /// @param wParam               Specifies the type of action the user performed.
    /// @param lParam               Specifies additional message-specific information.
    static INT_PTR ActionProc(
        HWND hwndDlg,
        UINT message,
        WPARAM wParam,
        LPARAM lParam);

    /// @brief Allow the dialog to clean up any resources that it needs to.
    ///
    /// @param in_dialogHandle      The handle to the dialog that created in_dialog.
    static void Dispose(HWND in_dialogHandle);

    /// @brief Display the dialog box on the screen
    ///
    /// @param in_dialogHandle      The handle to the dialog that created in_dialog.
    /// @param in_moduleHandle      The handle of the module.
    /// @param in_configSettings    The settings modified by the dialog.
/// @param edit_DSN				True if the DSN name should be editable
    ///
    /// @return Returns true if the user OKed the dialog; false, if they cancelled it.
    static bool Show(
        HWND in_parentWindow, 
        gemfirexd_handle in_moduleHandle,
        ConfigSettings& in_configSettings,
        bool edit_DSN);
        
// Private =====================================================================================
private:
    /// @brief Initialize all of the components of the dialog.
    ///
    /// @param in_dialogHandle      The handle to the dialog that created in_dialog.
    /// @param in_configSettings    The settings for this dialog.
    static void Initialize(HWND in_dialogHandle, ConfigSettings* in_configSettings);

    /// @brief Action taken when users click on the cancel button.
    ///
    /// @param in_dialogHandle      The handle to the dialog that created in_dialog.
    static void DoCancelAction(HWND in_dialogHandle);

    /// @brief Action taken when users click on the ok button.
    ///
    /// @param in_dialogHandle      The handle to the dialog that created in_dialog.
    /// @param in_configSettings    The settings for this dialog.
    static void DoOkAction(HWND in_dialogHandle, ConfigSettings* in_configSettings);

    /// @brief Action taken when users click on the brows button.
    ///
    /// @param in_dialogHandle      The handle to the dialog that created in_dialog.
    static void DoBrowseAction(HWND in_dialogHandle);

    /// @brief Enable or disable the OK button based on the contents of the edit controls.
    ///
    /// @param in_dialogHandle      The handle to the dialog that created in_dialog.
    static void CheckEnableOK(HWND in_dialogHandle);

    /// @brief Centers the dialog in the screen.
    ///
    /// @param in_dialogHandle      The handle to the dialog to be centered.
    static void CenterDialog(HWND in_dialogHandle);

    /// @brief Fetches the trimmed text from an Edit component.
    ///
    /// @param in_component         The component identifier to get the text from.
    /// @param in_dialogHandle      The handle to the dialog.
    ///
    /// @return The text from the specified component.
		static std::string GetEditText(int in_component, HWND in_dialogHandle);

    /// @brief Trim a string of the indicated characters.
    ///
    /// @param in_string                The string to trim.
    /// @param in_what                  The characters to trim from the beginning and end of 
    ///                                 the string.
    ///
    /// @return A new trimmed string.
    static std::string Trim(const std::string& in_string, const std::string& in_what);
};


#endif
