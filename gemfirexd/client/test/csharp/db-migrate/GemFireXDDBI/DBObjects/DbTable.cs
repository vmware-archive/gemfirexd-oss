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
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Pivotal.Data.GemFireXD;
using GemFireXDDBI.DBI;
using GemFireXDDBI.Util;
using GemFireXDDBI.DBMigrate;


namespace GemFireXDDBI.DBObjects
{
    /// <summary>
    /// Represents database table attributes 
    /// </summary>
    class DbTable : IComparable<DbTable>
    {
        public String SchemaName { get; set; }
        public String TableName { get; set; }
        public IList<DbField> Columns { get; set; }
        public String SelfRefColumn { get; set; }
        public IList<String> Constraints { get; set; }

        public String TableFullName { get { return String.Format("{0}.{1}", SchemaName, TableName); } }
        public Boolean Created { get; set; }

        private IList<String> referencedTables;


        public DbTable(String schemaName, String tableName)
        {
            this.Columns = new List<DbField>();
            this.Constraints = new List<String>();
            this.referencedTables = new List<String>();
            this.SelfRefColumn = String.Empty;
            this.SchemaName = schemaName.ToUpper();
            this.TableName = tableName.ToUpper();

            this.BuildCreateTableStatement();
        }

        private void BuildCreateTableStatement()
        {
            AddColumns();
            AddPKConstraint();
            AddFKConstraints();
            AddCKConstraints();
        }

        public void Migrate()
        {            
            if (!GemFireXDDbi.SchemaExists(SchemaName))
            {
                Migrator.OnMigrateEvent(new MigrateEventArgs(
                        Result.Unknown, String.Format("Migrating database schema {0}...", SchemaName)));

                GemFireXDDbi.Create(String.Format("CREATE SCHEMA {0}", this.SchemaName));                
            }

            if ((!this.Created) && (!GemFireXDDbi.TableExists(this.TableName)))
            {
                foreach (String tblFullName in referencedTables)
                {
                    if (!this.TableFullName.Equals(tblFullName))
                        Migrator.GetTableFromList(tblFullName).Migrate();
                }

                Migrator.OnMigrateEvent(new MigrateEventArgs(
                        Result.Unknown, String.Format("Creating table {0}...", TableFullName)));

                Helper.Log(GetCreateTableSql());
                GemFireXDDbi.Create(GetCreateTableSql());
                this.Created = true;

                Helper.Log(String.Format("Table {0} created", TableFullName));

                MigrateData();
            }         
        }

        private void MigrateData()
        {
            Helper.Log(String.Format("Start migrating table {0}", TableFullName));

            DataTable sqlTable = null;

            Migrator.OnMigrateEvent(new MigrateEventArgs(
                        Result.Unknown, String.Format("Copying table {0} data...", TableFullName)));

            sqlTable = SQLSvrDbi.GetTableData(TableFullName);

            GFXDClientConnection connection = GemFireXDDbi.OpenNewConnection();
            GemFireXDDbi.BatchInsert(connection, GetInsertSql(), sqlTable, this);

            while (connection.State == ConnectionState.Executing)
            {
                Helper.Log("Connection is still executing");
                Thread.Sleep(5000);
            }

            Validate(sqlTable);

            Helper.Log(String.Format("Table {0} migrated", TableFullName));            
        }

        public bool Validate(DataTable origTable)
        {
            Migrator.OnMigrateEvent(new MigrateEventArgs(
                            Result.Unknown, String.Format("Validating table {0} data...", TableFullName)));
            
            DataTable destTable = GemFireXDDbi.GetTableData(TableFullName);

            for (int i = 0; i < origTable.Rows.Count; i++)
            {
                for (int j = 0; j < origTable.Columns.Count; j++)
                    if (!Validate(destTable.Rows[i][j], origTable.Rows[i][j]))
                    {
                        Helper.Log(String.Format(
                            "Validate failed on table {0}, row {1}, column {2}", TableFullName, i, j));

                        Helper.Log(String.Format("dest -> {0}; orig -> {1}", 
                            destTable.Rows[i][j].ToString(), origTable.Rows[i][j].ToString()));

                        Migrator.Errorred = true;
                        return false;
                    }
            }

            Helper.Log(String.Format("Table {0} validated", TableFullName));
            return true;
        }

        public bool Validate(object orig, object dest)
        {
            if (orig is byte[])
                return (Helper.ConvertToBytes(dest) == Helper.ConvertToBytes(orig));
            else if (orig is int)
                return (int.Parse(dest.ToString()) == int.Parse(orig.ToString()));
            else if (orig is long)
                return (long.Parse(dest.ToString()) == long.Parse(orig.ToString()));
            else if (orig is float)
                return (float.Parse(dest.ToString()) == float.Parse(orig.ToString()));
            else if (orig is double)
                return (double.Parse(dest.ToString()) == double.Parse(orig.ToString()));
            else if (orig is decimal)
                return (decimal.Parse(dest.ToString()) == decimal.Parse(orig.ToString()));
            else if (orig is DateTime)
                return (DateTime.Parse(dest.ToString()) == DateTime.Parse(orig.ToString()));
            else if (orig is string)
                return (dest.ToString() == orig.ToString());
            else
                return true;                
        }

        public bool Validate()
        {           
            return GemFireXDDbi.TableExists(TableName);
        }

        private void AddColumns()
        {
            DataTable dt = SQLSvrDbi.GetTableColumns(SchemaName, TableName);
            if (dt != null & dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    DbField field = new DbField();

                    String colName = row["ColumnName"].ToString().Trim();

                    if (GemFireXDDbi.IsReservedWord(colName))
                        colName = String.Format("\"{0}\"", colName);
                    if (colName.Contains(" "))
                        colName = colName.Replace(" ", "_");

                    field.FieldName = colName;

                    if (!String.IsNullOrEmpty(row["CharMaxLength"].ToString()))
                        field.Length = Convert.ToInt32(row["CharMaxLength"].ToString());

                    field.FieldType = DbTypeMap.GetGFXDTypeFromSqlType(row["DataType"].ToString());

                    if (row["IsNullable"].ToString().Trim() == "YES")
                        field.IsNullable = true;

                    Columns.Add(field);
                }
            }         
        }

        private void AddPKConstraint()
        {
            DataTable dt = SQLSvrDbi.GetPKConstraint(SchemaName, TableName);
            StringBuilder constraint = new StringBuilder();

            if (dt != null & dt.Rows.Count > 0)
            {
                constraint.AppendFormat("CONSTRAINT {0} PRIMARY KEY (",
                    dt.Rows[0]["ConstraintName"].ToString());

                foreach (DataRow row in dt.Rows)
                    constraint.AppendFormat("{0}, ", row["ColumnName"].ToString());

                constraint = constraint.Remove(constraint.Length - 2, 1).Append(") ");
                Constraints.Add(constraint.ToString());
            }     
        }

        private void AddFKConstraints()
        {            
            DataTable dt = SQLSvrDbi.GetFKConstraints(SchemaName, TableName);

            if (dt != null & dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    String referencedTable = String.Format("{0}.{1}",
                        SQLSvrDbi.GetTableSchema(row["ReferencedTableName"].ToString()),
                        row["ReferencedTableName"].ToString().ToUpper());

                    for (int i = 0; i < Constraints.Count; i++)
                    {
                        if (Constraints[i].Contains(row["ConstraintName"].ToString()))
                        {
                            int startIndex = Constraints[i].IndexOf(row["ConstraintName"].ToString());
                            int endIndex = Constraints[i].IndexOf(')', startIndex);

                            Constraints[i] = Constraints[i].Insert(
                                endIndex, String.Format(", {0}", row["ColumnName"].ToString()));

                            startIndex = Constraints[i].IndexOf(')', startIndex) + 1;
                            endIndex = Constraints[i].IndexOf(')', startIndex);
                            Constraints[i] = Constraints[i].Insert(
                                endIndex, String.Format(", {0}", row["ReferencedColumnName"].ToString()));

                            return;
                        }
                    }

                    Constraints.Add(String.Format("CONSTRAINT {0} FOREIGN KEY ({1}) REFERENCES {2}({3}) ",
                            row["ConstraintName"].ToString(), row["ColumnName"].ToString(),
                            referencedTable, row["ReferencedColumnName"].ToString()));

                    referencedTables.Add(referencedTable);

                    if (referencedTable == TableFullName)
                        SelfRefColumn = row["ColumnName"].ToString();
                }
            }         
        }

        private void AddCKConstraints()
        {            
            DataTable dt = SQLSvrDbi.GetCKConstraints(SchemaName, TableName);

            if (dt != null & dt.Rows.Count > 0)
            {
                foreach (DataRow row in dt.Rows)
                {
                    String constrDef = row["ConstraintDefinition"].ToString().Replace(
                        "[", String.Empty).Replace("]", String.Empty);

                    if (constrDef.Contains("year") || constrDef.Contains("getdate()"))  // Ignore this constraint
                        continue;

                    //if (constrDef.Contains("year"))
                    //    constrDef = constrDef.Replace("year", "YEAR(CURRENT_DATE)");
                    //if (constrDef.Contains("getdate()"))
                    //    constrDef = constrDef.Replace("getdate()", "CURRENT_DATE");                    

                    if (row["ConstraintName"].ToString() == "CK_ProductInventory_Shelf")
                        continue;

                    Constraints.Add(String.Format(
                        "CONSTRAINT {0} CHECK {1} ", row["ConstraintName"].ToString(), constrDef));
                }
            }         
        }

        public String GetCreateTableSql()
        {
            StringBuilder sql = new StringBuilder();

            sql.Append("CREATE TABLE ");
            sql.AppendFormat("{0} (", TableFullName);

            foreach (DbField field in Columns)
                sql.AppendFormat("{0} {1} {2}, ",
                    field.FieldName, DbTypeMap.GetJdbcTypeFromGFXDType(field.FieldType, field.Length),
                    (field.IsNullable ? String.Empty : "NOT NULL"));

            foreach (String constraint in Constraints)
                sql.AppendFormat("{0}, ", constraint);

            sql = sql.Remove(sql.Length - 2, 1).Append(") ");

            return sql.ToString();
        }

        public String GetInsertSql()
        {
            StringBuilder sql = new StringBuilder();

            sql.AppendFormat("INSERT INTO {0} (", TableFullName);
            foreach (DbField field in Columns)
                sql.AppendFormat("{0}, ", field.FieldName);

            sql = sql.Remove(sql.Length - 2, 1).Append(") ");
            sql.Append("VALUES (");

            foreach (DbField field in Columns)
                sql.Append("?, ");

            sql = sql.Remove(sql.Length - 2, 1).Append(") ");

            return sql.ToString();
        }

        #region IComparable<AwTable> Members

        public int CompareTo(DbTable other)
        {
            if (this.referencedTables.Contains(other.TableName))
                return 1;
            else if (other.referencedTables.Contains(this.TableName))
                return -1;

            return 0;
        }

        #endregion
    }
}
