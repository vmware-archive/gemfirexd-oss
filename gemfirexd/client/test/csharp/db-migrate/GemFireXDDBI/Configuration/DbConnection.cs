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
using GemFireXDDBI.DBObjects;
using System.Configuration;

namespace GemFireXDDBI.Configuration
{
    /// <summary>
    /// User interface for viewing and changing connection string properties
    /// </summary>
    public partial class DbConnection : Form
    {
        private IList<DbConn> dbConns;

        public DbConnection()
        {
            InitializeComponent();
            LoadConnections();
        }

        private void LoadConnections()
        {
            IList<ConnectionStringSettings> conns = Configuration.Configurator.GetDBConnStrings();
            dbConns = new List<DbConn>();

            foreach (ConnectionStringSettings setting in conns)
            {
                dbConns.Add(new DbConn(setting.Name, setting.ConnectionString, setting.ProviderName));
            }

            dataGridViewConnStrs.DataSource = dbConns;
        }

        private void buttonProperties_Click(object sender, EventArgs e)
        {
            ConnectionStringSettings setting = Configuration.Configurator.GetDBConnSetting(
                dataGridViewConnStrs.SelectedCells[0].OwningRow.Cells[0].Value.ToString());

            ConnProps connProps = new ConnProps(setting);

            if (connProps.ShowDialog() == DialogResult.OK)
            {
                Configuration.Configurator.RemoveDBConnString(setting.Name);

                ConnectionStringSettings newSetting = new ConnectionStringSettings(
                    connProps.ConnStrSettings.Name, connProps.ConnStrSettings.ConnectionString,
                    connProps.ConnStrSettings.ProviderName);

                Configuration.Configurator.AddDBConnString(newSetting);
            }
        }

        private void buttonAdd_Click(object sender, EventArgs e)
        {
            AddConn addConn = new AddConn();

            if (addConn.ShowDialog() == DialogResult.OK)
            {
                Configuration.Configurator.AddDBConnString(addConn.ConnStrSettings);
                LoadConnections();
            }
        }

        private void buttonRemove_Click(object sender, EventArgs e)
        {
            if (MessageBox.Show("Remove connection?", "DB Connection",
                MessageBoxButtons.YesNo, MessageBoxIcon.Question) == DialogResult.Yes)
            {
                Configuration.Configurator.RemoveDBConnString(
                    dataGridViewConnStrs.SelectedCells[0].OwningRow.Cells[0].Value.ToString());

                LoadConnections();
            }
        }

        private void buttonOK_Click(object sender, EventArgs e)
        {
            DialogResult = DialogResult.OK;
            this.Close();
        }

        private void buttonCancel_Click(object sender, EventArgs e)
        {
            DialogResult = DialogResult.Cancel;
            this.Close();
        }
    }
}
