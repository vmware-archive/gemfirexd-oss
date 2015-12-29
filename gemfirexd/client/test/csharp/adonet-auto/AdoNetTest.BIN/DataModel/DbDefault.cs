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
using System.Linq;
using System.Text;
using System.IO;
using Pivotal.Data.GemFireXD;
using AdoNetTest.BIN.Configuration;
using AdoNetTest.BIN.BusinessObjects;
using AdoNetTest.BIN.DataObjects;

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Structures and methods for creating and operating on the the default commerce test database 
    /// </summary>
    public enum TableName
    {
        ADDRESS,
        SUPPLIER,
        CATEGORY,
        PRODUCT,
        CUSTOMER,
        ORDERS,
        ORDERDETAIL
    }

    class DbDefault
    {
        private static readonly long addressPKeySeed = long.Parse(GFXDConfigManager.GetDbDefaultSetting("addressPKeySeed"));
        private static readonly long supplierPKeySeed = long.Parse(GFXDConfigManager.GetDbDefaultSetting("supplierPKeySeed"));
        private static readonly long categoryPKSeed = long.Parse(GFXDConfigManager.GetDbDefaultSetting("categoryPKSeed"));
        private static readonly long productPKSeed = long.Parse(GFXDConfigManager.GetDbDefaultSetting("productPKSeed"));
        private static readonly long customerPKSeed = long.Parse(GFXDConfigManager.GetDbDefaultSetting("customerPKSeed"));
        private static readonly long orderPKSeed = long.Parse(GFXDConfigManager.GetDbDefaultSetting("orderPKSeed"));

        private static readonly long numSuppliers = long.Parse(GFXDConfigManager.GetDbDefaultSetting("numSuppliers"));
        private static readonly long numCategoriesPerSupplier = long.Parse(GFXDConfigManager.GetDbDefaultSetting("numCategoriesPerSupplier"));
        private static readonly long numProductsPerCategory = long.Parse(GFXDConfigManager.GetDbDefaultSetting("numProductsPerCategory"));
        private static readonly long numCustomers = long.Parse(GFXDConfigManager.GetDbDefaultSetting("numCustomers"));
        private static readonly long numOrdersPerCustomer = long.Parse(GFXDConfigManager.GetDbDefaultSetting("numOrdersPerCustomer"));
        private static readonly long numOrderDetailsPerOrder = long.Parse(GFXDConfigManager.GetDbDefaultSetting("numOrderDetailsPerOrder"));
        

        private static readonly IDictionary<TableName, DbTable> tblStructures
            = new Dictionary<TableName, DbTable>
            {
                {
                    TableName.ADDRESS, new DbTable("ADDRESS",
                        new List<String>{"address_id"},
                        new List<String>(),
                        new List<DbField>{
                            new DbField("address_id", GFXDType.Long, 9), 
                            new DbField("address1", GFXDType.VarChar, 20),
                            new DbField("address2", GFXDType.VarChar, 10),
                            new DbField("address3", GFXDType.VarChar, 10),
                            new DbField("city", GFXDType.VarChar, 10),
                            new DbField("state", GFXDType.VarChar, 10),
                            new DbField("zip_code", GFXDType.VarChar, 5),
                            new DbField("province", GFXDType.VarChar, 20),
                            new DbField("country_code", GFXDType.Integer)
                        },
                        new List<String>{
                                "CONSTRAINT pk_address PRIMARY KEY (address_id)"},
                        addressPKeySeed)
                },
                {
                    TableName.SUPPLIER, new DbTable("SUPPLIER",
                        new List<String>{"supplier_id"},
                        new List<String>{"address_id"},
                        new List<DbField>{
                            new DbField("supplier_id", GFXDType.Long, 9),
                            new DbField("supplier_name", GFXDType.VarChar, 50),
                            new DbField("address_id", GFXDType.Long, 9),
                            new DbField("phone", GFXDType.VarChar, 25),
                            new DbField("email", GFXDType.VarChar, 50)
                        }, 
                        new List<String>{
                            "CONSTRAINT pk_supplier PRIMARY KEY (supplier_id)",
                            "CONSTRAINT fk_address FOREIGN KEY (address_id) REFERENCES address(address_id)"},
                        supplierPKeySeed)
                },
                {
                    TableName.CATEGORY, new DbTable("CATEGORY",
                        new List<String>{"category_id"},
                        new List<String>(),
                        new List<DbField>{
                            new DbField("category_id", GFXDType.Long, 9),
                            new DbField("category_name", GFXDType.VarChar, 50),
                            new DbField("category_description", GFXDType.VarChar, 500)
                        },
                        new List<String>{
                            "CONSTRAINT pk_category PRIMARY KEY (category_id)"},
                        categoryPKSeed)
                },
                {
                    TableName.PRODUCT, new DbTable("PRODUCT",
                        new List<String>{"product_id"},
                        new List<String>{"category_id", "supplier_id"},
                        new List<DbField>{
                            new DbField("product_id", GFXDType.Long, 9),
                            new DbField("product_name", GFXDType.VarChar, 50),
                            new DbField("product_description", GFXDType.VarChar, 200),
                            new DbField("category_id", GFXDType.Long, 9),
                            new DbField("supplier_id", GFXDType.Long, 9),
                            new DbField("unit_cost", GFXDType.Decimal, 7),
                            new DbField("retail_price", GFXDType.Decimal, 7),
                            new DbField("units_in_stock", GFXDType.Integer),
                            new DbField("reorder_quantity", GFXDType.Integer),
                            new DbField("last_order_date", GFXDType.Date),
                            new DbField("next_order_date", GFXDType.Date)
                        },
                        new List<String>{
                            "CONSTRAINT pk_product PRIMARY KEY (product_id)",
                            "CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES category(category_id)",
                            "CONSTRAINT fk_supplier FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id)"},
                        productPKSeed)
                },
                {
                    TableName.CUSTOMER, new DbTable("CUSTOMER",
                        new List<String>{"customer_id"},
                        new List<String>{"address_id"},
                        new List<DbField>{
                            new DbField("customer_id", GFXDType.Long, 9),
                            new DbField("first_name", GFXDType.VarChar, 100),
                            new DbField("last_name", GFXDType.VarChar, 100),
                            new DbField("address_id", GFXDType.Long, 9),
                            new DbField("phone", GFXDType.VarChar, 25),
                            new DbField("email", GFXDType.VarChar, 100),
                            new DbField("last_order_date", GFXDType.Date)
                        },
                        new List<String>{
                            "CONSTRAINT pk_customer PRIMARY KEY (customer_id)",
                            "CONSTRAINT fk_addresss FOREIGN KEY (address_id) REFERENCES address(address_id)"},
                        customerPKSeed)
                },
                {
                    TableName.ORDERS, new DbTable("ORDERS",
                        new List<String>{"order_id"},
                        new List<String>{"customer_id"},
                        new List<DbField>{
                            new DbField("order_id", GFXDType.Long, 9),
                            new DbField("order_date", GFXDType.Date),
                            new DbField("ship_date", GFXDType.Date),
                            new DbField("customer_id", GFXDType.Long, 9),
                            new DbField("subtotal", GFXDType.Decimal, 7)
                        },
                        new List<String>{
                            "CONSTRAINT pk_order PRIMARY KEY (order_id)",
                            "CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES customer(customer_id)"},
                        orderPKSeed)
                },
                {
                    TableName.ORDERDETAIL, new DbTable("ORDERDETAIL",
                        new List<String>{"order_id", "product_id"},
                        new List<String>{"order_id", "product_id"},
                        new List<DbField>{
                            new DbField("order_id", GFXDType.Long, 9),
                            new DbField("product_id", GFXDType.Long, 9),
                            new DbField("quantity", GFXDType.Integer),
                            new DbField("unit_price", GFXDType.Decimal, 7),
                            new DbField("discount", GFXDType.Decimal, 7)
                        },
                        new List<String>{
                            "CONSTRAINT pk_orders_product PRIMARY KEY (order_id, product_id)",
                            "CONSTRAINT fk_orders FOREIGN KEY (order_id) REFERENCES orders(order_id)",
                            "CONSTRAINT fk_product FOREIGN KEY (product_id) REFERENCES product(product_id)"},
                        0)
                }
            };

        private static readonly IDictionary<Relation, DbRelation> tblRelations
            = new Dictionary<Relation, DbRelation>
            {
                {Relation.SUPPLIER_ADDRESS , new DbRelation(TableName.SUPPLIER, TableName.ADDRESS, "address_id")},
                {Relation.CUSTOMER_ADDRESS, new DbRelation(TableName.CUSTOMER, TableName.ADDRESS, "address_id")},
                {Relation.PRODUCT_CATEGORY, new DbRelation(TableName.PRODUCT, TableName.CATEGORY, "category_id")},
                {Relation.PRODUCT_SUPPLIER, new DbRelation(TableName.PRODUCT, TableName.SUPPLIER, "supplier_id")},
                {Relation.ORDER_CUSTOMER, new DbRelation(TableName.ORDERS, TableName.CUSTOMER, "customer_id")},
                {Relation.ORDERDETAIL_ORDER, new DbRelation(TableName.ORDERDETAIL, TableName.ORDERS, "order_id")},
                {Relation.ORDERDETAIL_PRODUCT, new DbRelation(TableName.ORDERDETAIL, TableName.PRODUCT, "product_id")}
            };
        
        public static String GetCreateTableBasicStatement(TableName tableName)
        {
            DbTable table = GetTableStructure(tableName);
            IList<DbField> fields = table.Columns;
            IList<String> constraints = table.Constraints;
            StringBuilder sql = new StringBuilder();

            sql.Append("CREATE TABLE ");
            sql.AppendFormat("{0} (", tableName.ToString());

            foreach (DbField field in fields)
                sql.AppendFormat("{0} {1} NOT NULL, ", 
                    field.FieldName, DbTypeMap.GetGFXDType(field.FieldType, field.Length));

            foreach (String constraint in constraints)
                sql.AppendFormat("{0}, ", constraint);

            sql = sql.Remove(sql.Length - 2, 1).Append(") ");

            return sql.ToString();
        }

        public static String GetCreateTablePKPartitionStatement(TableName tableName)
        {
            return String.Format("{0} PARTITION BY PRIMARY KEY ", GetCreateTableBasicStatement(tableName));
        }

        public static String GetCreateTableReplicateStatement(TableName tableName)
        {
            return String.Format("{0} REPLICATE ", GetCreateTableBasicStatement(tableName));
        }

        public static String GetCreateTableFKPartitionStatement(Relation relation)
        {
            DbRelation rel = GetTableRelation(relation);
            StringBuilder sql = new StringBuilder(GetCreateTableBasicStatement(rel.PrimaryTableName));

            if (String.IsNullOrEmpty(rel.FKey))
                return sql.ToString();

            sql.AppendFormat(" PARTITION BY COLUMN ({0}) ", rel.FKey);

            return sql.ToString();
        }

        public static String GetCreateTablePKPartitionColocateStatement(Relation relation)
        {
            DbRelation rel = GetTableRelation(relation);
            StringBuilder sql = new StringBuilder(GetCreateTablePKPartitionStatement(rel.PrimaryTableName));

            sql.AppendFormat(" COLOCATE WITH ({0}) ", rel.ForeignTableName);

            return sql.ToString();
        }

        public static String GetCreateTableFKPartitionColocateStatement(Relation relation)
        {
            DbRelation rel = GetTableRelation(relation);            
            StringBuilder sql = new StringBuilder(GetCreateTableFKPartitionStatement(relation));

            sql.AppendFormat(" COLOCATE WITH ({0}) ", rel.ForeignTableName);

            return sql.ToString();
        }

        public static String GetSelectStatement(TableName tableName)
        {
            IList<DbField> fields = GetTableStructure(tableName).Columns;
            StringBuilder sql = new StringBuilder();

            sql.Append("SELECT ");
            for (int j = 0; j < fields.Count; j++)
                sql.Append(fields[j].FieldName + ", ");

            sql = sql.Remove(sql.Length - 2, 1);
            sql.Append("FROM " + tableName.ToString());

            return sql.ToString();
        }

        public static String GetSelectStatement(TableName tableName, String condition)
        {
            StringBuilder sql = new StringBuilder(GetSelectStatement(tableName));

            if(!String.IsNullOrEmpty(condition))
                sql.AppendFormat(" WHERE {0} ", condition);

            return sql.ToString();
        }

        public static String GetSelectStatement(TableName tableName, long[] pKey)
        {
            StringBuilder sql = new StringBuilder(GetSelectStatement(tableName));
            IList<String> pkFields = GetTableStructure(tableName).PKColumns;

            if (pKey.Length > 0)
            {
                sql.AppendFormat(" WHERE {0} = {1} ", pkFields[0], pKey[0]);
                for (int j = 1; j < pKey.Length; j++)
                    sql.AppendFormat("AND {0} = {1} ", pkFields[j], pKey[j]);
            }

            return sql.ToString();
        }

        public static String GetSelectCountStatement(TableName tableName)
        {
            return String.Format("SELECT COUNT(*) FROM {0}", tableName.ToString());
        }

        public static String GetSelectRandomStatement(TableName tableName, int numRows)
        {
            return String.Format(
                "SELECT * FROM {0} ORDER BY RANDOM() FETCH FIRST {1} ROWS ONLY",
                tableName.ToString(), numRows);
        }

        public static String GetInnerJoinSelectStatement(Relation relation, long[] pKey)
        {
            DbRelation tblRelation = GetTableRelation(relation);
            IList<String> pkFields = GetTableStructure(tblRelation.PrimaryTableName).PKColumns;

            StringBuilder sql = GetJoinSelectStatement(relation);            

            sql.AppendFormat(" INNER JOIN {0} ON {1}.{2} = {3}.{4}", 
                tblRelation.ForeignTableName.ToString(),
                tblRelation.PrimaryTableName.ToString(), tblRelation.FKey,
                tblRelation.ForeignTableName.ToString(), tblRelation.FKey);

            if (pKey != null && pKey.Length > 0)
            {
                sql.AppendFormat(" WHERE {0}.{1} = {2} ", 
                    tblRelation.ForeignTableName.ToString(),tblRelation.FKey, pKey[0]);
                for (int j = 1; j < pKey.Length; j++)
                    sql.AppendFormat("AND {0}.{1} = {2} ", 
                        tblRelation.PrimaryTableName.ToString(), pkFields[j], pKey[j]);
            }

            return sql.ToString();
        }

        public static String GetLeftOuterJoinSelectStatement(Relation relation, String condition)
        {
            DbRelation tblRelation = GetTableRelation(relation);
            IList<String> pkFields = GetTableStructure(tblRelation.PrimaryTableName).PKColumns;

            StringBuilder sql = GetJoinSelectStatement(relation);            

            sql.AppendFormat(" LEFT OUTER JOIN {0} ON {1}.{2} = {3}.{4}",
                tblRelation.ForeignTableName.ToString(),
                tblRelation.PrimaryTableName.ToString(), tblRelation.FKey,
                tblRelation.ForeignTableName.ToString(), tblRelation.FKey);

            if (!String.IsNullOrEmpty(condition))
                sql.AppendFormat(" WHERE {0} ", condition);

            return sql.ToString();
        }


        public static String GetCreateViewStatement(Relation relation)
        {
            StringBuilder sql = new StringBuilder();
            DbRelation tblRelation = GetTableRelation(relation);
            IList<DbField> pTblFields = GetTableStructure(tblRelation.PrimaryTableName).Columns;
            IList<DbField> fTblFields = GetTableStructure(tblRelation.ForeignTableName).Columns;

            sql.AppendFormat("CREATE VIEW {0} (", relation.ToString());
            foreach (DbField field in pTblFields)
                sql.AppendFormat("P_{0}, ", field.FieldName);

            foreach (DbField field in fTblFields)
                sql.AppendFormat("F_{0}, ", field.FieldName);

            sql = sql.Remove(sql.Length - 2, 1);

            sql.AppendFormat(") AS {0}", GetInnerJoinSelectStatement(relation, null));

            return sql.ToString();
        }

        private static StringBuilder GetJoinSelectStatement(Relation relation)
        {
            StringBuilder sql = new StringBuilder();
            DbRelation tblRelation = GetTableRelation(relation);
            IList<DbField> pTblFields = GetTableStructure(tblRelation.PrimaryTableName).Columns;
            IList<DbField> fTblFields = GetTableStructure(tblRelation.ForeignTableName).Columns;

            sql.Append("SELECT ");

            foreach (DbField field in pTblFields)
                sql.AppendFormat("{0}.{1}, ",
                    tblRelation.PrimaryTableName.ToString(), field.FieldName);

            foreach (DbField field in fTblFields)
                sql.AppendFormat("{0}.{1}, ",
                    tblRelation.ForeignTableName.ToString(), field.FieldName);

            sql = sql.Remove(sql.Length - 2, 1);

            sql.AppendFormat(" FROM {0} ", tblRelation.PrimaryTableName.ToString());

            return sql;
        }

        public static String GetInsertStatement(TableName tableName, long[] pKey)
        {
            IList<DbField> fields = GetTableStructure(tableName).Columns;
            StringBuilder sql = new StringBuilder();

            sql.AppendFormat("INSERT INTO {0}(", tableName.ToString());
            for (int i = 0; i < fields.Count; i++)
                sql.Append(fields[i].FieldName + ", ");

            sql = sql.Remove(sql.Length - 2, 1).Append(")");

            if (pKey.Length > 0)
            {
                sql.AppendFormat(" VALUES( {0}, ", pKey[0]);
                for (int j = 1; j < pKey.Length; j++)
                    sql.AppendFormat("{0}, ", pKey[j]);

                for (int k = pKey.Length; k < fields.Count; k++) 
                    sql.AppendFormat("{{{0}}}, ", k - pKey.Length);
            }

            return sql.Remove(sql.Length - 2, 1).Append(")").ToString();
        }


        public static String GetUpdateStatement(TableName tableName, long[] pKey)
        {
            IList<DbField> fields = GetTableStructure(tableName).Columns;
            IList<String> pkFields = GetTableStructure(tableName).PKColumns;
            StringBuilder sql = new StringBuilder();

            sql.AppendFormat("UPDATE {0} SET ", tableName.ToString());

            for (int i = pKey.Length; i < fields.Count; i++)
                sql.AppendFormat("{0} = {{{1}}}, ", fields[i].FieldName, i - pKey.Length);

            sql = sql.Remove(sql.Length - 2, 1);

            if (pKey.Length > 0)
            {
                sql.AppendFormat("WHERE {0} = {1} ", fields[0].FieldName, pKey[0]);
                for (int j = 1; j < pKey.Length; j++)
                    sql.AppendFormat("AND {0} = {1} ", pkFields[j], pKey[j]);
            }

            return sql.ToString();
        }

        public static String GetDeleteStatement(TableName tableName)
        {
            IList<DbField> fields = GetTableStructure(tableName).Columns;
            StringBuilder sql = new StringBuilder();

            sql.AppendFormat("DELETE FROM {0} ", tableName.ToString());

            return sql.ToString();
        }

        public static String GetDeleteStatement(TableName tableName, String condition)
        {
            StringBuilder sql = new StringBuilder(GetDeleteStatement(tableName));

            if (!String.IsNullOrEmpty(condition))
                sql.AppendFormat(" WHERE {0} ", condition);

            return sql.ToString();
        }

        public static String GetDeleteStatement(TableName tableName, long[] primaryKey)
        {
            IList<String> pkFields = GetTableStructure(tableName).PKColumns;
            StringBuilder sql = new StringBuilder(GetDeleteStatement(tableName));

            if (primaryKey.Length > 0)
            {
                sql.AppendFormat(" WHERE {0} = {1} ", pkFields[0], primaryKey[0]);
                for (int j = 1; j < primaryKey.Length; j++)
                    sql.AppendFormat("AND {0} = {1} ", pkFields[j], primaryKey[j]);
            }

            return sql.ToString();
        }

        public static String GetDropTableStatement(TableName tableName)
        {
            return String.Format("DROP TABLE {0} ", tableName.ToString());
        }

        public static DbTable GetTableStructure(TableName tableName)
        {
            return (DbTable)tblStructures[tableName];
        }

        public static String GetTableName(TableName tbEnum)
        {
            return tblStructures[tbEnum].TableName;
        }

        public static DbRelation GetTableRelation(Relation relation)
        {
            return tblRelations[relation];
        }

        /// <summary>
        /// Returns the default select statement for address table
        /// </summary>
        /// <returns></returns>
        public static String GetAddressQuery()
        {
            return GetSelectStatement(TableName.ADDRESS);
        }

        /// <summary>
        /// Returns the default supplier query statement
        /// </summary>
        /// <returns></returns>
        public static String GetSupplierQuery()
        {
            return GetSelectStatement(TableName.SUPPLIER);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static String GetCategoryQuery()
        {
            return GetSelectStatement(TableName.CATEGORY);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static String GetProductQuery()
        {
            return GetSelectStatement(TableName.PRODUCT);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static String GetCustomerQuery()
        {
            return GetSelectStatement(TableName.CUSTOMER);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static String GetOrderQuery()
        {
            return GetSelectStatement(TableName.ORDERS);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public static String GetOrderDetailQuery()
        {
            return GetSelectStatement(TableName.ORDERDETAIL);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        public static String GetCreateTableBasicStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, GetCreateTableBasicStatement(tableName));
        }

        public static String GetCreateTablePKPartitionStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, GetCreateTablePKPartitionStatement(tableName));
        }

        public static String GetCreateTableReplicateStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, GetCreateTableReplicateStatement(tableName));
        }

        public static String GetCreateTableFKPartitionStatement(String schemaName, Relation relation)
        {
            return QualifyTableName(schemaName, GetCreateTableFKPartitionStatement(relation));
        }

        public static String GetCreateTablePKPartitionColocateStatement(String schemaName, Relation relation)
        {
            return QualifyTableName(schemaName, GetCreateTablePKPartitionColocateStatement(relation));
        }

        public static String GetCreateTableFKPartitionColocateStatement(String schemaName, Relation relation)
        {
            return QualifyTableName(schemaName, GetCreateTableFKPartitionColocateStatement(relation));
        }

        public static String GetSelectStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, GetSelectStatement(tableName));
        }

        public static String GetSelectStatement(String schemaName, TableName tableName, String condition)
        {
            return QualifyTableName(schemaName, GetSelectStatement(tableName, condition));
        }

        public static String GetSelectStatement(String schemaName, TableName tableName, long[] pKey)
        {
            return QualifyTableName(schemaName, GetSelectStatement(tableName, pKey));
        }

        public static String GetSelectCountStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, GetSelectCountStatement(tableName));
        }

        public static String GetInnerJoinSelectStatement(String schemaName, Relation relation, long[] pKey)
        {
            return QualifyTableName(schemaName, GetInnerJoinSelectStatement(relation, pKey));
        }

        public static String GetLeftOuterJoinSelectStatement(String schemaName, Relation relation, String condition)
        {
            return QualifyTableName(schemaName, GetLeftOuterJoinSelectStatement(relation, condition));
        }

        public static StringBuilder GetJoinSelectStatement(String schemaName, Relation relation)
        {
            return new StringBuilder(QualifyTableName(schemaName, GetJoinSelectStatement(relation).ToString()));
        }

        public static String GetInsertStatement(String schemaName, TableName tableName, long[] pKey)
        {
            return QualifyTableName(schemaName, GetInsertStatement(tableName, pKey));
        }

        public static String GetUpdateStatement(String schemaName, TableName tableName, long[] pKey)
        {
            return QualifyTableName(schemaName, GetUpdateStatement(tableName, pKey));
        }

        public static String GetDeleteStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, GetDeleteStatement(tableName));
        }

        public static String GetDeleteStatement(String schemaName, TableName tableName, String condition)
        {
            return QualifyTableName(schemaName, GetDeleteStatement(tableName, condition));
        }

        public static String GetDeleteStatement(String schemaName, TableName tableName, long[] primaryKey)
        {
            return QualifyTableName(schemaName, GetDeleteStatement(tableName, primaryKey));
        }

        public static String GetDropTableStatement(String schemaName, TableName tableName)
        {
            return QualifyTableName(schemaName, GetDropTableStatement(tableName));
        }

        public static String GetTableName(String schemaName, TableName tableName)
        {
            return String.Format("{0}.{1}", schemaName, tableName);
        }
        
        public static String GetAddressQuery(String schemaName)
        {
            return QualifyTableName(schemaName, GetSelectStatement(TableName.ADDRESS));
        }

        public static String GetSupplierQuery(String schemaName)
        {
            return QualifyTableName(schemaName, GetSelectStatement(TableName.SUPPLIER));
        }

        public static String GetCategoryQuery(String schemaName)
        {
            return QualifyTableName(schemaName, GetSelectStatement(TableName.CATEGORY));
        }

        public static String GetProductQuery(String schemaName)
        {
            return QualifyTableName(schemaName, GetSelectStatement(TableName.PRODUCT));
        }

        public static String GetCustomerQuery(String schemaName)
        {
            return QualifyTableName(schemaName, GetSelectStatement(TableName.CUSTOMER));
        }

        public static String GetOrderQuery(String schemaName)
        {
            return QualifyTableName(schemaName, GetSelectStatement(TableName.ORDERS));
        }

        public static String GetOrderDetailQuery(String schemaName)
        {
            return QualifyTableName(schemaName, GetSelectStatement(TableName.ORDERDETAIL));
        }

        public static void CreateDB(String schemaName, DbCreateType dbType)
        {
            if (!String.IsNullOrEmpty(schemaName))
                DbHelper.CreateSchema(schemaName);

            CreateTables(schemaName, dbType);
            PopulateTables(schemaName);
        }

        private static void CreateTables(String schemaName, DbCreateType dbType)
        {
            switch (dbType)
            {
                case DbCreateType.Basic:
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableBasicStatement(
                        schemaName, TableName.ADDRESS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableBasicStatement(
                        schemaName, TableName.CATEGORY));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableBasicStatement(
                        schemaName, TableName.SUPPLIER));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableBasicStatement(
                        schemaName, TableName.CUSTOMER));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableBasicStatement(
                        schemaName, TableName.PRODUCT));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableBasicStatement(
                        schemaName, TableName.ORDERS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableBasicStatement(
                        schemaName, TableName.ORDERDETAIL));
                    return;
                case DbCreateType.PKPartition:
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.ADDRESS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.CATEGORY));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.SUPPLIER));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.CUSTOMER));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.PRODUCT));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.ORDERS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.ORDERDETAIL));
                    return;
                case DbCreateType.Colocate:
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.ADDRESS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTablePKPartitionStatement(
                        schemaName, TableName.CATEGORY));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableFKPartitionColocateStatement(
                        schemaName, Relation.SUPPLIER_ADDRESS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableFKPartitionColocateStatement(
                        schemaName, Relation.CUSTOMER_ADDRESS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableFKPartitionColocateStatement(
                        schemaName, Relation.PRODUCT_CATEGORY));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableFKPartitionColocateStatement(
                        schemaName, Relation.ORDER_CUSTOMER));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableReplicateStatement(
                        schemaName, TableName.ORDERDETAIL));
                    return;
                case DbCreateType.FKPartition:
                    return;
                case DbCreateType.Replicate:
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableReplicateStatement(
                        schemaName, TableName.ADDRESS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableReplicateStatement(
                        schemaName, TableName.CATEGORY));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableReplicateStatement(
                        schemaName, TableName.SUPPLIER));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableReplicateStatement(
                        schemaName, TableName.CUSTOMER));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableReplicateStatement(
                        schemaName, TableName.PRODUCT));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableReplicateStatement(
                        schemaName, TableName.ORDERS));
                    DbHelper.ExecuteNonQueryStatement(GetCreateTableReplicateStatement(
                        schemaName, TableName.ORDERDETAIL));
                    return;
            }
        }

        public static void DropDB(String schemaName)
        {
            DropTables(schemaName); 
        }

        private static void DropTables(String schemaName)
        {
            DbHelper.ExecuteNonQueryStatement(GetDropTableStatement(
                schemaName, TableName.ORDERDETAIL));
            DbHelper.ExecuteNonQueryStatement(GetDropTableStatement(
                schemaName, TableName.ORDERS));
            DbHelper.ExecuteNonQueryStatement(GetDropTableStatement(
                schemaName, TableName.PRODUCT));
            DbHelper.ExecuteNonQueryStatement(GetDropTableStatement(
                schemaName, TableName.CUSTOMER));
            DbHelper.ExecuteNonQueryStatement(GetDropTableStatement(
                schemaName, TableName.CATEGORY));
            DbHelper.ExecuteNonQueryStatement(GetDropTableStatement(
                schemaName, TableName.SUPPLIER));
            DbHelper.ExecuteNonQueryStatement(GetDropTableStatement(
                schemaName, TableName.ADDRESS));
        }

        private static String QualifyTableName(String schemaName, String sql)
        {
            if (String.IsNullOrEmpty(schemaName))
                return sql;

            foreach (TableName tname in Enum.GetValues(typeof(TableName)))
            {
                String tnameTok = String.Format("{0}", tname.ToString());

                if (sql.Contains(tnameTok))
                    sql = sql.Replace(tnameTok, String.Format("{0}.{1}",
                        schemaName, tname.ToString()));
            }

            return sql;
        }

        private static void PopulateTables(String schemaName)
        {
            DbController dbc = null;

            if (String.IsNullOrEmpty(schemaName))
                dbc = new DbController(DbHelper.OpenNewConnection());
            else
                dbc = new DbController(DbHelper.OpenNewConnection(), schemaName);

            IList<Product> prodList = new List<Product>();
            long startAddressId = addressPKeySeed;
            long startSupplierId = supplierPKeySeed;
            long startCategoryId = categoryPKSeed;
            long startProductId = productPKSeed;
            long startCustomerId = customerPKSeed;
            long startOrderId = orderPKSeed;

            for (int i = 0; i < numSuppliers; i++)
            {
                Address address = (Address)ObjectFactory.Create(ObjectType.Address);
                address.AddressId = startAddressId++;
                dbc.AddAddress(address);

                Supplier supplier = (Supplier)ObjectFactory.Create(ObjectType.Supplier);
                supplier.SupplierId = startSupplierId++;
                supplier.Address = address;
                dbc.AddSupplier(supplier);

                for (int j = 0; j < numCategoriesPerSupplier; j++)
                {
                    Category category = (Category)ObjectFactory.Create(ObjectType.Category);
                    category.CategoryId = startCategoryId++;
                    dbc.AddCategory(category);

                    for (int k = 0; k < numProductsPerCategory; k++)
                    {
                        Product product = (Product)ObjectFactory.Create(ObjectType.Product);
                        product.ProductId = startProductId++;
                        product.Category = category;
                        product.Supplier = supplier;
                        dbc.AddProduct(product);

                        // to be referenced by OrderDetail
                        prodList.Add(product);
                    }
                }
            }

            int prodCount = 0;
            for (int i = 0; i < numCustomers; i++)
            {
                Address address = (Address)ObjectFactory.Create(ObjectType.Address);
                address.AddressId = startAddressId++;
                dbc.AddAddress(address);

                Customer customer = (Customer)ObjectFactory.Create(ObjectType.Customer);
                customer.CustomerId = startCustomerId++;
                customer.Address = address;
                dbc.AddCustomer(customer);

                for (int j = 0; j < numOrdersPerCustomer; j++)
                {
                    Order order = (Order)ObjectFactory.Create(ObjectType.Order);
                    order.OrderId = startOrderId++;
                    order.Customer = customer;
                    dbc.AddOrder(order);

                    for (int k = 0; k < numOrderDetailsPerOrder; k++)
                    {
                        OrderDetail ordDetail = (OrderDetail)ObjectFactory.Create(ObjectType.OrderDetail);
                        ordDetail.OrderId = order.OrderId;
                        ordDetail.ProductId = prodList[prodCount].ProductId;
                        ordDetail.UnitPrice = prodList[prodCount++].RetailPrice;
                        dbc.AddOrderDetail(ordDetail);

                        if (prodCount == prodList.Count)
                            prodCount = 0;
                    }
                }
            }

            //ShowTablesData(schemaName);
        }

        public static void ShowTablesData(String schemaName)
        {
            DbController dbc = null;

            if (String.IsNullOrEmpty(schemaName))
                dbc = new DbController(DbHelper.OpenNewConnection());
            else
                dbc = new DbController(DbHelper.OpenNewConnection(), schemaName);

            IList<Address> addresses = dbc.GetAddresses();
            foreach (Address addr in addresses)
                GFXDTestRunner.OnTestEvent(new TestEventArgs("Address: " + addr.ToString()));

            IList<Supplier> suppliers = dbc.GetSuppliers();
            foreach (Supplier supp in suppliers)
                GFXDTestRunner.OnTestEvent(new TestEventArgs("Supplier: " + supp.ToString()));

            IList<Category> categories = dbc.GetCategories();
            foreach (Category cat in categories)
                GFXDTestRunner.OnTestEvent(new TestEventArgs("Category: " + cat.ToString()));

            IList<Product> products = dbc.GetProducts();
            foreach (Product prod in products)
                GFXDTestRunner.OnTestEvent(new TestEventArgs("Product: " + prod.ToString()));

            IList<Customer> customers = dbc.GetCustomers();
            foreach (Customer cust in customers)
                GFXDTestRunner.OnTestEvent(new TestEventArgs("Customer: " + cust.ToString()));

            IList<Order> orders = dbc.GetOrders();
            foreach (Order ord in orders)
                GFXDTestRunner.OnTestEvent(new TestEventArgs("Order: " + ord.ToString()));

            IList<OrderDetail> ordDetails = dbc.GetOrderDetails();
            foreach (OrderDetail ordDetail in ordDetails)
                GFXDTestRunner.OnTestEvent(new TestEventArgs("OrderDetail: " + ordDetail.ToString()));
        }
    }
}
