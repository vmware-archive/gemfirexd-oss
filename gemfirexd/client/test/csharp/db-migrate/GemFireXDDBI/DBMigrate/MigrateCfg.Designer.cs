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
 
namespace GemFireXDDBI.DBMigrate
{
    partial class MigrateCfg
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.groupBox1 = new System.Windows.Forms.GroupBox();
            this.checkBoxTriggers = new System.Windows.Forms.CheckBox();
            this.checkBoxFunctions = new System.Windows.Forms.CheckBox();
            this.checkBoxStoredProcedures = new System.Windows.Forms.CheckBox();
            this.checkBoxViews = new System.Windows.Forms.CheckBox();
            this.checkBoxTables = new System.Windows.Forms.CheckBox();
            this.comboBoxDestDB = new System.Windows.Forms.ComboBox();
            this.comboBoxSourceDB = new System.Windows.Forms.ComboBox();
            this.label3 = new System.Windows.Forms.Label();
            this.label2 = new System.Windows.Forms.Label();
            this.label1 = new System.Windows.Forms.Label();
            this.buttonOK = new System.Windows.Forms.Button();
            this.buttonCancel = new System.Windows.Forms.Button();
            this.checkBoxIndexes = new System.Windows.Forms.CheckBox();
            this.groupBox1.SuspendLayout();
            this.SuspendLayout();
            // 
            // groupBox1
            // 
            this.groupBox1.Controls.Add(this.checkBoxIndexes);
            this.groupBox1.Controls.Add(this.checkBoxTriggers);
            this.groupBox1.Controls.Add(this.checkBoxFunctions);
            this.groupBox1.Controls.Add(this.checkBoxStoredProcedures);
            this.groupBox1.Controls.Add(this.checkBoxViews);
            this.groupBox1.Controls.Add(this.checkBoxTables);
            this.groupBox1.Controls.Add(this.comboBoxDestDB);
            this.groupBox1.Controls.Add(this.comboBoxSourceDB);
            this.groupBox1.Controls.Add(this.label3);
            this.groupBox1.Controls.Add(this.label2);
            this.groupBox1.Controls.Add(this.label1);
            this.groupBox1.Location = new System.Drawing.Point(13, 22);
            this.groupBox1.Name = "groupBox1";
            this.groupBox1.Size = new System.Drawing.Size(433, 175);
            this.groupBox1.TabIndex = 0;
            this.groupBox1.TabStop = false;
            this.groupBox1.Text = "Specify options for database migartion";
            // 
            // checkBoxTriggers
            // 
            this.checkBoxTriggers.AutoSize = true;
            this.checkBoxTriggers.Location = new System.Drawing.Point(187, 128);
            this.checkBoxTriggers.Name = "checkBoxTriggers";
            this.checkBoxTriggers.Size = new System.Drawing.Size(64, 17);
            this.checkBoxTriggers.TabIndex = 9;
            this.checkBoxTriggers.Text = "Triggers";
            this.checkBoxTriggers.UseVisualStyleBackColor = true;
            // 
            // checkBoxFunctions
            // 
            this.checkBoxFunctions.AutoSize = true;
            this.checkBoxFunctions.Location = new System.Drawing.Point(100, 128);
            this.checkBoxFunctions.Name = "checkBoxFunctions";
            this.checkBoxFunctions.Size = new System.Drawing.Size(72, 17);
            this.checkBoxFunctions.TabIndex = 8;
            this.checkBoxFunctions.Text = "Functions";
            this.checkBoxFunctions.UseVisualStyleBackColor = true;
            // 
            // checkBoxStoredProcedures
            // 
            this.checkBoxStoredProcedures.AutoSize = true;
            this.checkBoxStoredProcedures.Location = new System.Drawing.Point(274, 104);
            this.checkBoxStoredProcedures.Name = "checkBoxStoredProcedures";
            this.checkBoxStoredProcedures.Size = new System.Drawing.Size(114, 17);
            this.checkBoxStoredProcedures.TabIndex = 7;
            this.checkBoxStoredProcedures.Text = "Stored Procedures";
            this.checkBoxStoredProcedures.UseVisualStyleBackColor = true;
            // 
            // checkBoxViews
            // 
            this.checkBoxViews.AutoSize = true;
            this.checkBoxViews.Location = new System.Drawing.Point(187, 104);
            this.checkBoxViews.Name = "checkBoxViews";
            this.checkBoxViews.Size = new System.Drawing.Size(54, 17);
            this.checkBoxViews.TabIndex = 6;
            this.checkBoxViews.Text = "Views";
            this.checkBoxViews.UseVisualStyleBackColor = true;
            // 
            // checkBoxTables
            // 
            this.checkBoxTables.AutoSize = true;
            this.checkBoxTables.Location = new System.Drawing.Point(100, 104);
            this.checkBoxTables.Name = "checkBoxTables";
            this.checkBoxTables.Size = new System.Drawing.Size(58, 17);
            this.checkBoxTables.TabIndex = 5;
            this.checkBoxTables.Text = "Tables";
            this.checkBoxTables.UseVisualStyleBackColor = true;
            // 
            // comboBoxDestDB
            // 
            this.comboBoxDestDB.FormattingEnabled = true;
            this.comboBoxDestDB.Location = new System.Drawing.Point(100, 70);
            this.comboBoxDestDB.Name = "comboBoxDestDB";
            this.comboBoxDestDB.Size = new System.Drawing.Size(327, 21);
            this.comboBoxDestDB.TabIndex = 4;
            // 
            // comboBoxSourceDB
            // 
            this.comboBoxSourceDB.FormattingEnabled = true;
            this.comboBoxSourceDB.Location = new System.Drawing.Point(100, 35);
            this.comboBoxSourceDB.Name = "comboBoxSourceDB";
            this.comboBoxSourceDB.Size = new System.Drawing.Size(327, 21);
            this.comboBoxSourceDB.TabIndex = 3;
            // 
            // label3
            // 
            this.label3.AutoSize = true;
            this.label3.Location = new System.Drawing.Point(6, 104);
            this.label3.Name = "label3";
            this.label3.Size = new System.Drawing.Size(82, 13);
            this.label3.TabIndex = 2;
            this.label3.Text = "Include Entities:";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(6, 70);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(81, 13);
            this.label2.TabIndex = 1;
            this.label2.Text = "Destination DB:";
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(6, 35);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(62, 13);
            this.label1.TabIndex = 0;
            this.label1.Text = "Source DB:";
            // 
            // buttonOK
            // 
            this.buttonOK.Location = new System.Drawing.Point(279, 214);
            this.buttonOK.Name = "buttonOK";
            this.buttonOK.Size = new System.Drawing.Size(75, 23);
            this.buttonOK.TabIndex = 1;
            this.buttonOK.Text = "OK";
            this.buttonOK.UseVisualStyleBackColor = true;
            this.buttonOK.Click += new System.EventHandler(this.buttonOK_Click);
            // 
            // buttonCancel
            // 
            this.buttonCancel.Location = new System.Drawing.Point(371, 214);
            this.buttonCancel.Name = "buttonCancel";
            this.buttonCancel.Size = new System.Drawing.Size(75, 23);
            this.buttonCancel.TabIndex = 2;
            this.buttonCancel.Text = "Cancel";
            this.buttonCancel.UseVisualStyleBackColor = true;
            this.buttonCancel.Click += new System.EventHandler(this.buttonCancel_Click);
            // 
            // checkBoxIndexes
            // 
            this.checkBoxIndexes.AutoSize = true;
            this.checkBoxIndexes.Location = new System.Drawing.Point(274, 128);
            this.checkBoxIndexes.Name = "checkBoxIndexes";
            this.checkBoxIndexes.Size = new System.Drawing.Size(63, 17);
            this.checkBoxIndexes.TabIndex = 10;
            this.checkBoxIndexes.Text = "Indexes";
            this.checkBoxIndexes.UseVisualStyleBackColor = true;
            // 
            // MigrateCfg
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(459, 262);
            this.Controls.Add(this.buttonCancel);
            this.Controls.Add(this.buttonOK);
            this.Controls.Add(this.groupBox1);
            this.Name = "MigrateCfg";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterParent;
            this.Text = "DBMigrate";
            this.groupBox1.ResumeLayout(false);
            this.groupBox1.PerformLayout();
            this.ResumeLayout(false);

        }

        #endregion

        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.CheckBox checkBoxTriggers;
        private System.Windows.Forms.CheckBox checkBoxFunctions;
        private System.Windows.Forms.CheckBox checkBoxStoredProcedures;
        private System.Windows.Forms.CheckBox checkBoxViews;
        private System.Windows.Forms.CheckBox checkBoxTables;
        private System.Windows.Forms.ComboBox comboBoxDestDB;
        private System.Windows.Forms.ComboBox comboBoxSourceDB;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Button buttonOK;
        private System.Windows.Forms.Button buttonCancel;
        private System.Windows.Forms.CheckBox checkBoxIndexes;
    }
}