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
 
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Configuration;

namespace GemFireXDDBI.Configuration
{
    /// <summary>
    /// User interface for adding new connection string
    /// </summary>
    public partial class AddConn : Form
    {
        public ConnectionStringSettings ConnStrSettings { get; set; }

        public AddConn()
        {
            InitializeComponent();
        }

        private void buttonOK_Click(object sender, EventArgs e)
        {
            if (!String.IsNullOrEmpty(textBoxName.Text) &&
                !String.IsNullOrEmpty(textBoxConnString.Text) &&
                !String.IsNullOrEmpty(textBoxProvider.Text))
            {
                ConnStrSettings = new ConnectionStringSettings(textBoxName.Text.Trim(), 
                    textBoxConnString.Text.Trim(), textBoxProvider.Text.Trim());

                DialogResult = DialogResult.OK;
                this.Close();
            }

            else
            MessageBox.Show("Please specified all connection values", 
                "Add Connection", MessageBoxButtons.OK, MessageBoxIcon.Error);
        }

        private void buttonCancel_Click(object sender, EventArgs e)
        {
            DialogResult = DialogResult.Cancel;
            this.Close();
        }
    }
}
