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
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GemFireXDDBI.DBI
{
    /// <summary>
    /// Specific implementation of SQLServer interface
    /// </summary>
    class SQLSvrDbi
    {
        public static string DbConnectionString { get; set; }

        public static int TestConnection()
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            connection.Open();

            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
                return 1;
            }

            return 0;
        }

        public static DataTable GetTableNames()
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();
            SqlCommand command = connection.CreateCommand();
            command.CommandText = @"SELECT TABLE_SCHEMA AS SchemaName, TABLE_NAME AS TableName
                                    FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'
                                    ORDER BY TABLE_SCHEMA ASC";

            SqlDataAdapter adapter = new SqlDataAdapter(command);

            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }

        public static DataTable GetTableColumns(string schemaName, string tableName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = String.Format(
                @"SELECT COLUMN_NAME AS ColumnName, DATA_TYPE AS DataType, 
                CHARACTER_MAXIMUM_LENGTH AS CharMaxLength, NUMERIC_PRECISION As NumericPrecision,
                IS_NULLABLE IsNullable, COLUMN_DEFAULT AS ColumnDefault
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'", schemaName, tableName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);

            connection.Close();
            
            return dt;
        }


        public static DataTable GetPKConstraint(string schemaName, string tableName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = String.Format(
                @"SELECT  OBJECT_NAME(IDXC.object_id) AS TableName, IDX.name AS ConstraintName , 
                COL_NAME(IDXC.object_id, IDXC.column_id) AS ColumnName FROM sys.indexes AS IDX 
                INNER JOIN sys.index_columns AS IDXC ON IDX.object_id = IDXC.object_id  
                AND IDX.index_id = IDXC.index_id 
                INNER JOIN sys.objects OBJ ON IDX.object_id = OBJ.object_id
                WHERE IDX.is_primary_key = 1
                AND SCHEMA_NAME(OBJ.schema_id)= '{0}'
                AND OBJECT_NAME(IDXC.object_id) = '{1}'",
                schemaName, tableName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }

        public static DataTable GetFKConstraints(string schemaName, string tableName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = String.Format(
                @"SELECT OBJECT_NAME(FKEYC.parent_object_id) AS TableName, FKEY.name AS ConstraintName,
                COL_NAME(FKEYC.parent_object_id, FKEYC.parent_column_id) AS ColumnName,
                OBJECT_NAME(FKEYC.referenced_object_id) ReferencedTableName,
                COL_NAME(FKEYC.referenced_object_id, FKEYC.referenced_column_id) AS ReferencedColumnName
                FROM sys.foreign_key_columns FKEYC
                INNER JOIN sys.foreign_keys FKEY ON FKEYC.constraint_object_id = FKEY.object_id
                WHERE SCHEMA_NAME(FKEY.schema_id) = '{0}' AND OBJECT_NAME(FKEYC.parent_object_id) = '{1}'",
                schemaName, tableName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);

            connection.Close();
            
            return dt;
        }

        public static DataTable GetCKConstraints(string schemaName, string tableName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = String.Format(
                @"SELECT OBJ.name AS TableName, CHKC.name AS ConstraintName, 
                CHKC.definition AS ConstraintDefinition FROM sys.check_constraints CHKC
                INNER JOIN sys.objects OBJ ON CHKC.parent_object_id = OBJ.object_id
                INNER JOIN sys.schemas SCH ON OBJ.schema_id = SCH.schema_id
                WHERE SCH.name = '{0}' AND OBJ.name = '{1}'",
                schemaName, tableName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }

        public static String GetTableSchema(string tableName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            string schemaName = string.Empty;

            connection.Open();
            SqlCommand command = connection.CreateCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = String.Format(
                        @"SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME='{0}'", tableName);

            schemaName = Convert.ToString(command.ExecuteScalar());
        
            connection.Close();
            
            return schemaName.ToUpper();
        }

        public static DataTable GetIndexes()
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = @"SELECT S.name AS SchemaName, T.name AS TableName, I.name AS IndexName, 
                                    I.type_desc AS TypeDescription, I.is_unique AS IsUnique,
                                    C.name AS ColumnName FROM sys.tables T
                                    INNER JOIN sys.schemas S ON T.schema_id = S.schema_id
                                    INNER JOIN sys.indexes I ON I.object_id = T.object_id 
                                    INNER JOIN sys.index_columns IC ON IC.object_id = T.object_id 
                                    INNER JOIN sys.columns C ON C.object_id = T.object_id
                                    AND IC.index_id = I.index_id AND IC.column_id = C.column_id";

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
                    
            connection.Close();
            
            return dt;
        }

        public static DataTable GetIndex(string indexName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            string[] temp = indexName.Split(new char[] { '.' });
            indexName = temp[temp.Length - 1];
                        
            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = String.Format(@"SELECT S.name AS SchemaName, T.name AS TableName, I.name AS IndexName, 
                                    I.type_desc AS TypeDescription, I.is_unique AS IsUnique,
                                    C.name AS ColumnName FROM sys.tables T
                                    INNER JOIN sys.schemas S ON T.schema_id = S.schema_id
                                    INNER JOIN sys.indexes I ON I.object_id = T.object_id 
                                    INNER JOIN sys.index_columns IC ON IC.object_id = T.object_id 
                                    INNER JOIN SYS.COLUMNS C ON C.object_id = T.object_id
                                    AND IC.index_id = I.index_id AND IC.column_id = C.column_id 
                                    WHERE I.name = '{0}'", indexName);
                            
            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
                    
            connection.Close();
            
            return dt;
        }

        public static DataTable GetStoredProcedures()
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = @"SELECT ROUTINE_SCHEMA AS SchemaName, ROUTINE_NAME AS ProcedureName, 
                                    ROUTINE_DEFINITION AS ProcedureDefinition
                                    FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE'";

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }

        public static DataTable GetStoredProcedure(string procedureName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            string [] temp = procedureName.Split(new char[] { '.' });
            procedureName = temp[temp.Length - 1];

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = string.Format(@"SELECT * FROM INFORMATION_SCHEMA.ROUTINES 
                                                WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_NAME = '{0}'", 
                                                procedureName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }

        public static DataTable GetFunctions()
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = @"SELECT ROUTINE_SCHEMA AS SchemaName, ROUTINE_NAME AS FunctionName, 
                                    ROUTINE_DEFINITION AS FunctionDefinition
                                    FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_TYPE = 'FUNCTION'";

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }

        public static DataTable GetFunction(string functionName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            string[] temp = functionName.Split(new char[] { '.' });
            functionName = temp[temp.Length - 1];

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = string.Format(@"SELECT * FROM INFORMATION_SCHEMA.ROUTINES 
                                                WHERE ROUTINE_TYPE = 'FUNCTION' AND ROUTINE_NAME = '{0}'", 
                                                functionName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);

            connection.Close();

            return dt;
        }

        public static DataTable GetTriggers()
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = @"SELECT SCH.name AS SchemaName, OBJ.name AS TriggerName, 
                                    SMOD.definition AS TriggerDefinition FROM sys.objects OBJ 
                                    INNER JOIN sys.sql_modules SMOD ON OBJ.object_id = SMOD.object_id 
                                    INNER JOIN sys.schemas SCH ON OBJ.schema_id = SCH.schema_id
                                    WHERE OBJ.type = 'TR'";

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);

            connection.Close();

            return dt;
        }

        public static DataTable GetTrigger(string triggerName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            string[] temp = triggerName.Split(new char[] { '.' });
            triggerName = temp[temp.Length - 1];

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = String.Format(@"SELECT SCH.name AS SchemaName, OBJ.name AS TriggerName, 
                                    SMOD.* FROM sys.objects OBJ 
                                    INNER JOIN sys.sql_modules SMOD ON OBJ.object_id = SMOD.object_id 
                                    INNER JOIN sys.schemas SCH ON OBJ.schema_id = SCH.schema_id
                                    WHERE OBJ.type = 'TR' AND OBJ.name = '{0}'", triggerName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }

        public static DataTable GetViews()
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = @"SELECT TABLE_SCHEMA AS SchemaName, TABLE_NAME AS ViewName, 
                                    VIEW_DEFINITION AS ViewDefinition FROM INFORMATION_SCHEMA.VIEWS ";

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }

        public static DataTable GetView(string viewName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = string.Format(
                        @"SELECT * FROM INFORMATION_SCHEMA.VIEWS 
                          WHERE TABLE_NAME = '{0}'", viewName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);

            connection.Close();
            
            return dt;
        }

        public static DataTable GetViewData(string viewName)
        {
            SqlConnection connection = new SqlConnection(DbConnectionString);
            DataTable dt = new DataTable();

            connection.Open();

            SqlCommand command = connection.CreateCommand();
            command.CommandText = string.Format(@"SELECT * FROM {0}", viewName);

            SqlDataAdapter adapter = new SqlDataAdapter(command);
            adapter.Fill(dt);
        
            connection.Close();
            
            return dt;
        }


        public static DataTable GetTableData(string tableName)
        {
            StringBuilder sql = new StringBuilder();
            sql.AppendFormat("SELECT * FROM {0} ORDER BY {1} ASC", 
                tableName, GetPKColumnName(tableName));

            Util.Helper.Log(sql.ToString());

            using (SqlConnection connection = new SqlConnection(DbConnectionString))
            {
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = System.Data.CommandType.Text;
                    command.CommandText = sql.ToString();

                    connection.Open();

                    using (SqlDataAdapter adapter = new SqlDataAdapter(command))
                    {
                        DataTable dt = new DataTable();
                        adapter.Fill(dt);
                        return dt;
                    }
                }
            }
        }

        public static String GetPKColumnName(string tableName)
        {
            string[] nameSplit = tableName.Split(new char[] { '.' });

            string sql = String.Format(@"SELECT C.COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS C
                                        WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'
                                        AND ORDINAL_POSITION = 1", nameSplit[0], nameSplit[1]);

            return (String)ExecuteScalarStatement(new SqlConnection(DbConnectionString), sql);
        }

        public static object ExecuteScalarStatement(SqlConnection conn, String statement)
        {
            conn.Open();
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandType = CommandType.Text;
            cmd.CommandText = statement;

            return cmd.ExecuteScalar();
        }
    }
}
