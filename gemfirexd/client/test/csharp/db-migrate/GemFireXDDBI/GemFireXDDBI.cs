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
using System.Threading;

namespace GemFireXDDBI
{
    /// <summary>
    /// Main user interface
    /// </summary>
    public partial class GemFireXDDBI : Form
    {
        public GemFireXDDBI()
        {
            InitializeComponent();
        }

        private void migrateToGemFireXDToolStripMenuItem_Click(object sender, EventArgs e)
        {
            DBMigrate.MigrateCfg cfg = new DBMigrate.MigrateCfg();

            try
            {
                if (cfg.ShowDialog() == DialogResult.OK)
                {
                    DBMigrate.Progressor progessor = new DBMigrate.Progressor();
                    Thread pThread = new Thread(new ThreadStart(progessor.Run));
                    pThread.Start();
                    //pThread.Join();

                    if (progessor.DialogResult == DialogResult.Abort)
                        MessageBox.Show(
                            "Operation canceled!", "GemFireXDDBI", MessageBoxButtons.OK, MessageBoxIcon.Information);
                    else if (progessor.DialogResult == DialogResult.OK)
                    {
                        if (DBMigrate.Migrator.Errorred)
                            MessageBox.Show(
                                "Database migration completed with errors. Check log for details!", "GemFireXDDBI",
                                MessageBoxButtons.OK, MessageBoxIcon.Warning);
                        else
                            MessageBox.Show("Database migration completed successfully!", "GemFireXDDBI",
                                MessageBoxButtons.OK, MessageBoxIcon.Information);
                    }
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show("Encountered exception during database migration. Check log for detail",
                    "DB Migration Error", MessageBoxButtons.OK, MessageBoxIcon.Error);

                Util.Helper.Log(ex);
            }
        }

        private void openDBToolStripMenuItem_Click(object sender, EventArgs e)
        {
            OpenDB openDB = new OpenDB();

            try
            {
                if (openDB.ShowDialog() == DialogResult.OK)
                {
                    if (treeViewDB.Nodes[openDB.SelectedDBConn] != null)
                        CloseDB(openDB.SelectedDBConn);

                    LoadDB(openDB.SelectedDBConn);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(String.Format(
                    "Failed to open connection to {0}", openDB.SelectedDBConn), 
                    "DB Connection Error", MessageBoxButtons.OK, MessageBoxIcon.Error);

                Util.Helper.Log(ex);
            }
        }

        private void openToolStripButton_Click(object sender, EventArgs e)
        {
            openDBToolStripMenuItem_Click(sender, e);
        }

        private void LoadDB(String dbConnName)
        {
            DBI.SQLBase dbi = DBI.SQLFactory.GetSqlDBI(dbConnName);

            // Get tables
            DataTable dt = dbi.GetTableNames();
            TreeNode tnode1 = treeViewDB.Nodes.Add(dbConnName);
            tnode1.Name = dbConnName;
            tnode1.ContextMenuStrip = contextMenuStrip1;
            TreeNode tnode2 = tnode1.Nodes.Add("Tables");
            foreach (DataRow row in dt.Rows)
                tnode2.Nodes.Add(
                    String.Format("{0}.{1}", row[0].ToString(), row[1].ToString()));

            // Get views
            dt.Clear();
            dt = dbi.GetViews();
            tnode2 = tnode1.Nodes.Add("Views");
            foreach (DataRow row in dt.Rows)
                tnode2.Nodes.Add(
                    String.Format("{0}.{1}", row[0].ToString(), row[1].ToString()));

            // Get indexes
            dt.Clear();
            dt = dbi.GetIndexes();
            tnode2 = tnode1.Nodes.Add("Indexes");
            foreach (DataRow row in dt.Rows)
                tnode2.Nodes.Add(
                    String.Format("{0}.{1}.{2}", row[0].ToString(), row[1].ToString(), row[2].ToString()));

            // Get stored procedures
            dt.Clear();
            dt = dbi.GetStoredProcedures();
            tnode2 = tnode1.Nodes.Add("Procedures");
            foreach (DataRow row in dt.Rows)
                tnode2.Nodes.Add(
                    String.Format("{0}.{1}", row[0].ToString(), row[1].ToString()));

            // Get functions
            dt.Clear();
            dt = dbi.GetFunctions();
            tnode2 = tnode1.Nodes.Add("Functions");
            foreach (DataRow row in dt.Rows)
                tnode2.Nodes.Add(
                    String.Format("{0}.{1}", row[0].ToString(), row[1].ToString()));

            // Get triggers
            dt.Clear();
            dt = dbi.GetTriggers();
            tnode2 = tnode1.Nodes.Add("Triggers");
            foreach (DataRow row in dt.Rows)
                tnode2.Nodes.Add(
                        String.Format("{0}.{1}", row[0].ToString(), row[1].ToString()));
        }

        private void treeViewDB_NodeMouseClick(object sender, TreeNodeMouseClickEventArgs e)
        {
            if (e.Node.Parent == null)
                return;

            TreeNode tnode = e.Node;

            while(tnode.Parent != null)
                tnode = tnode.Parent;

            DBI.SQLBase dbi = DBI.SQLFactory.GetSqlDBI(tnode.Text);

            if (e.Node.Text == "Tables")
                dataGridViewDB.DataSource = dbi.GetTableNames();
            else if (e.Node.Text == "Views")
                ;
            else if (e.Node.Text == "Indexes")
                dataGridViewDB.DataSource = dbi.GetIndexes();
            else if (e.Node.Text == "Procedures")
                dataGridViewDB.DataSource = dbi.GetStoredProcedures();
            else if (e.Node.Text == "Functions")
                dataGridViewDB.DataSource = dbi.GetFunctions();
            else if (e.Node.Text == "Triggers")
                dataGridViewDB.DataSource = dbi.GetTriggers();
            else if (e.Node.Parent.Text == "Tables")            
                dataGridViewDB.DataSource = dbi.GetTableData(e.Node.Text);
            else if(e.Node.Parent.Text == "Views")
                dataGridViewDB.DataSource = dbi.GetViewData(e.Node.Text);
            else if (e.Node.Parent.Text == "Indexes")
                dataGridViewDB.DataSource = dbi.GetIndex(e.Node.Text);
            else if (e.Node.Parent.Text == "Procedures")
                dataGridViewDB.DataSource = dbi.GetStoredProcedure(e.Node.Text);
            else if (e.Node.Parent.Text == "Functions")
                dataGridViewDB.DataSource = dbi.GetFunction(e.Node.Text);
            else if (e.Node.Parent.Text == "Triggers")
                dataGridViewDB.DataSource = dbi.GetTrigger(e.Node.Text);
        }

        private void dataGridViewDB_DataError(object sender, DataGridViewDataErrorEventArgs e)
        {
            Util.Helper.Log(e.Exception);
        }

        private void CloseDB(String dbConnName)
        {
            treeViewDB.Nodes[dbConnName].Remove();
        }

        private void closeToolStripMenuItem1_Click(object sender, EventArgs e)
        {
            CloseDB(treeViewDB.SelectedNode.Name);
        }

        private void reloadToolStripMenuItem_Click(object sender, EventArgs e)
        {
            String dbConnName = treeViewDB.SelectedNode.Name;
            CloseDB(dbConnName);
            LoadDB(dbConnName);
        }

        private void treeViewDB_MouseDown(object sender, MouseEventArgs e)
        {
            if (e.Button == MouseButtons.Right)
                treeViewDB.SelectedNode = treeViewDB.GetNodeAt(e.X, e.Y);
        }

        private void exitToolStripMenuItem_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        private void dBConnectionsToolStripMenuItem_Click(object sender, EventArgs e)
        {
            Configuration.DbConnection dbConfig = new Configuration.DbConnection();

            dbConfig.ShowDialog();
        }

        private void loggingToolStripMenuItem_Click(object sender, EventArgs e)
        {

        }
    }
}
