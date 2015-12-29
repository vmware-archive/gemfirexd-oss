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
 
namespace AdoNetTest.WIN
{
    partial class formRunner
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
          this.menuStrip1 = new System.Windows.Forms.MenuStrip();
          this.fileToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
          this.toolStripConfigFile = new System.Windows.Forms.ToolStripMenuItem();
          this.toolStripSeparator1 = new System.Windows.Forms.ToolStripSeparator();
          this.exitToolStripMenuItem = new System.Windows.Forms.ToolStripMenuItem();
          this.listBoxMsg = new System.Windows.Forms.ListBox();
          this.buttonStart = new System.Windows.Forms.Button();
          this.buttonStop = new System.Windows.Forms.Button();
          this.radioButtonRunOnce = new System.Windows.Forms.RadioButton();
          this.radioButtonRunContinuous = new System.Windows.Forms.RadioButton();
          this.button1 = new System.Windows.Forms.Button();
          this.comboBox1 = new System.Windows.Forms.ComboBox();
          this.label1 = new System.Windows.Forms.Label();
          this.menuStrip1.SuspendLayout();
          this.SuspendLayout();
          // 
          // menuStrip1
          // 
          this.menuStrip1.Items.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.fileToolStripMenuItem});
          this.menuStrip1.Location = new System.Drawing.Point(0, 0);
          this.menuStrip1.Name = "menuStrip1";
          this.menuStrip1.Size = new System.Drawing.Size(717, 24);
          this.menuStrip1.TabIndex = 0;
          this.menuStrip1.Text = "menuStrip1";
          // 
          // fileToolStripMenuItem
          // 
          this.fileToolStripMenuItem.DropDownItems.AddRange(new System.Windows.Forms.ToolStripItem[] {
            this.toolStripConfigFile,
            this.toolStripSeparator1,
            this.exitToolStripMenuItem});
          this.fileToolStripMenuItem.Name = "fileToolStripMenuItem";
          this.fileToolStripMenuItem.Size = new System.Drawing.Size(35, 20);
          this.fileToolStripMenuItem.Text = "File";
          // 
          // toolStripConfigFile
          // 
          this.toolStripConfigFile.Name = "toolStripConfigFile";
          this.toolStripConfigFile.Size = new System.Drawing.Size(135, 22);
          this.toolStripConfigFile.Text = "Config File";
          this.toolStripConfigFile.Click += new System.EventHandler(this.toolStripConfigFile_Click);
          // 
          // toolStripSeparator1
          // 
          this.toolStripSeparator1.Name = "toolStripSeparator1";
          this.toolStripSeparator1.Size = new System.Drawing.Size(132, 6);
          // 
          // exitToolStripMenuItem
          // 
          this.exitToolStripMenuItem.Name = "exitToolStripMenuItem";
          this.exitToolStripMenuItem.Size = new System.Drawing.Size(135, 22);
          this.exitToolStripMenuItem.Text = "Exit";
          // 
          // listBoxMsg
          // 
          this.listBoxMsg.Dock = System.Windows.Forms.DockStyle.Bottom;
          this.listBoxMsg.FormattingEnabled = true;
          this.listBoxMsg.Location = new System.Drawing.Point(0, 114);
          this.listBoxMsg.Name = "listBoxMsg";
          this.listBoxMsg.Size = new System.Drawing.Size(717, 459);
          this.listBoxMsg.TabIndex = 6;
          // 
          // buttonStart
          // 
          this.buttonStart.Location = new System.Drawing.Point(531, 27);
          this.buttonStart.Name = "buttonStart";
          this.buttonStart.Size = new System.Drawing.Size(75, 23);
          this.buttonStart.TabIndex = 2;
          this.buttonStart.Text = "Start";
          this.buttonStart.UseVisualStyleBackColor = true;
          this.buttonStart.Click += new System.EventHandler(this.buttonStart_Click);
          // 
          // buttonStop
          // 
          this.buttonStop.Location = new System.Drawing.Point(612, 27);
          this.buttonStop.Name = "buttonStop";
          this.buttonStop.Size = new System.Drawing.Size(75, 23);
          this.buttonStop.TabIndex = 3;
          this.buttonStop.Text = "Stop";
          this.buttonStop.UseVisualStyleBackColor = true;
          this.buttonStop.Click += new System.EventHandler(this.buttonStop_Click);
          // 
          // radioButtonRunOnce
          // 
          this.radioButtonRunOnce.AutoSize = true;
          this.radioButtonRunOnce.Checked = true;
          this.radioButtonRunOnce.Location = new System.Drawing.Point(275, 30);
          this.radioButtonRunOnce.Name = "radioButtonRunOnce";
          this.radioButtonRunOnce.Size = new System.Drawing.Size(74, 17);
          this.radioButtonRunOnce.TabIndex = 4;
          this.radioButtonRunOnce.TabStop = true;
          this.radioButtonRunOnce.Text = "Run Once";
          this.radioButtonRunOnce.UseVisualStyleBackColor = true;
          this.radioButtonRunOnce.CheckedChanged += new System.EventHandler(this.radioButtonRunOnce_CheckedChanged);
          // 
          // radioButtonRunContinuous
          // 
          this.radioButtonRunContinuous.AutoSize = true;
          this.radioButtonRunContinuous.Location = new System.Drawing.Point(365, 30);
          this.radioButtonRunContinuous.Name = "radioButtonRunContinuous";
          this.radioButtonRunContinuous.Size = new System.Drawing.Size(108, 17);
          this.radioButtonRunContinuous.TabIndex = 5;
          this.radioButtonRunContinuous.Text = "Run Continuously";
          this.radioButtonRunContinuous.UseVisualStyleBackColor = true;
          this.radioButtonRunContinuous.CheckedChanged += new System.EventHandler(this.radioButtonRunContinuous_CheckedChanged);
          // 
          // button1
          // 
          this.button1.Location = new System.Drawing.Point(533, 66);
          this.button1.Name = "button1";
          this.button1.Size = new System.Drawing.Size(72, 27);
          this.button1.TabIndex = 8;
          this.button1.Text = "Run it";
          this.button1.UseVisualStyleBackColor = true;
          this.button1.Click += new System.EventHandler(this.button1_Click);
          // 
          // comboBox1
          // 
          this.comboBox1.FormattingEnabled = true;
          this.comboBox1.Location = new System.Drawing.Point(83, 66);
          this.comboBox1.MaxDropDownItems = 25;
          this.comboBox1.Name = "comboBox1";
          this.comboBox1.Size = new System.Drawing.Size(428, 21);
          this.comboBox1.Sorted = true;
          this.comboBox1.TabIndex = 9;
          this.comboBox1.SelectedIndexChanged += new System.EventHandler(this.comboBox1_SelectedIndexChanged);
          // 
          // label1
          // 
          this.label1.AutoSize = true;
          this.label1.Location = new System.Drawing.Point(17, 70);
          this.label1.Name = "label1";
          this.label1.Size = new System.Drawing.Size(60, 13);
          this.label1.TabIndex = 10;
          this.label1.Text = "Test name:";
          // 
          // formRunner
          // 
          this.ClientSize = new System.Drawing.Size(717, 573);
          this.Controls.Add(this.label1);
          this.Controls.Add(this.comboBox1);
          this.Controls.Add(this.button1);
          this.Controls.Add(this.radioButtonRunContinuous);
          this.Controls.Add(this.radioButtonRunOnce);
          this.Controls.Add(this.listBoxMsg);
          this.Controls.Add(this.buttonStop);
          this.Controls.Add(this.buttonStart);
          this.Controls.Add(this.menuStrip1);
          this.MainMenuStrip = this.menuStrip1;
          this.MaximumSize = new System.Drawing.Size(725, 600);
          this.MinimumSize = new System.Drawing.Size(725, 600);
          this.Name = "formRunner";
          this.Text = "AdoNetTest Runner";
          this.Load += new System.EventHandler(this.formRunner_Load);
          this.menuStrip1.ResumeLayout(false);
          this.menuStrip1.PerformLayout();
          this.ResumeLayout(false);
          this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.MenuStrip menuStrip1;
        private System.Windows.Forms.ToolStripMenuItem fileToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem exitToolStripMenuItem;
        private System.Windows.Forms.ToolStripMenuItem toolStripConfigFile;
        private System.Windows.Forms.ToolStripSeparator toolStripSeparator1;
        private System.Windows.Forms.ListBox listBoxMsg;
        private System.Windows.Forms.Button buttonStart;
        private System.Windows.Forms.Button buttonStop;
        private System.Windows.Forms.RadioButton radioButtonRunOnce;
        private System.Windows.Forms.RadioButton radioButtonRunContinuous;
        private System.Windows.Forms.Button button1;
        private System.Windows.Forms.ComboBox comboBox1;
        private System.Windows.Forms.Label label1;
    }
}

