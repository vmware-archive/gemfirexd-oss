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

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Enumeration for foreign key relationships between default test tables
    /// </summary>
    enum Relation
    {
        SUPPLIER_ADDRESS,
        CUSTOMER_ADDRESS,
        PRODUCT_CATEGORY,
        PRODUCT_SUPPLIER,
        ORDER_CUSTOMER,
        ORDERDETAIL_ORDER,
        ORDERDETAIL_PRODUCT
    }

    /// <summary>
    /// Foreign key relationship / constraint definition
    /// </summary>
    class DbRelation
    {
        public TableName PrimaryTableName { get; set; }
        public TableName ForeignTableName { get; set; }
        public String FKey { get; set; }

        public DbRelation(TableName priTblName, TableName forTblName, String fKey)
        {
            PrimaryTableName = priTblName;
            ForeignTableName = forTblName;
            FKey = fKey;
        }
    }
}
