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
using System.Data;
using System.Text;

namespace AdoNetTest.BIN.BusinessObjects
{
    /// <summary>
    /// Encapsulates product information with related supplier and product category
    /// </summary>
    public class Product : BusinessObject
    {

        public long ProductId
        {
            get;
            set;
        }
        public string Name
        {
            get;
            set;
        }
        public string Description
        {
            get;
            set;
        }

        private Category category;
        public Category Category
        {
            get
            {
                if (category == null)
                    category = new Category();

                return category;
            }
            set
            {
                category = value;
            }
        }

        private Supplier supplier;
        public Supplier Supplier
        {
            get
            {
                if (supplier == null)
                    supplier = new Supplier();

                return supplier;
            }
            set
            {
                supplier = value;
            }
        }
        public float UnitCost
        {
            get;
            set;
        }
        public float RetailPrice
        {
            get;
            set;
        }
        public int UnitsInStock
        {
            get;
            set;
        }
        public int ReorderQuantity
        {
            get;
            set;
        }
        public DateTime LastOrderDate
        {
            get;
            set;
        }
        public DateTime NextOrderDate
        {
            get;
            set;
        }

        public Product()
        {
            ProductId = 0;
            Name = String.Empty;
            Description = String.Empty;
            Category = new Category();
            Supplier = new Supplier();
            UnitCost = 0;
            RetailPrice = 0;
            UnitsInStock = 0;
            ReorderQuantity = 0;
            LastOrderDate = DateTime.Today;
            NextOrderDate = DateTime.Today;
        }

        public Product(long productId)
            : this()
        {
            ProductId = productId;
        }

        public Product(long productId, string name, string description,
            long categoryId, long supplierId, float unitCost, float retailPrice,
            int unitsInStock, int reorderQuantity, DateTime lastOrderDate, 
            DateTime nextOrderDate)
            : this()
        {
            ProductId = productId;
            Name = name;
            Description = description;
            Category.CategoryId = categoryId;
            Supplier.SupplierId = supplierId;
            UnitCost = unitCost;
            RetailPrice = retailPrice;
            UnitsInStock = unitsInStock;
            ReorderQuantity = reorderQuantity;
            LastOrderDate = lastOrderDate;
            NextOrderDate = nextOrderDate;
        }

        public Product(DataRow row)
            : this()
        {
            ProductId = long.Parse(row[0].ToString());
            Name = row[1].ToString();
            Description = row[2].ToString();
            Category.CategoryId = long.Parse(row[3].ToString());
            Supplier.SupplierId = long.Parse(row[4].ToString());
            UnitCost = float.Parse(row[5].ToString());
            RetailPrice = float.Parse(row[6].ToString());
            UnitsInStock = int.Parse(row[7].ToString());
            ReorderQuantity = int.Parse(row[8].ToString());
            LastOrderDate = DateTime.Parse(row[9].ToString());
            NextOrderDate = DateTime.Parse(row[10].ToString());
        }

        public override bool Equals(object obj)
        {
            if (!(obj is Product))
                return false;

            Product product = (Product)obj;

            return (this.ProductId == product.ProductId
                && this.Name == product.Name
                && this.Description == product.Description
                && this.Category.CategoryId == product.Category.CategoryId
                && this.Supplier.SupplierId == product.Supplier.SupplierId
                && this.UnitCost == product.UnitCost
                && this.RetailPrice == product.RetailPrice
                && this.UnitsInStock == product.UnitsInStock
                && this.ReorderQuantity == product.ReorderQuantity
                && this.LastOrderDate == product.LastOrderDate
                && this.NextOrderDate == product.NextOrderDate);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return String.Format(
                "{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}\n",
                this.ProductId, this.Name, this.Description,
                this.Category.CategoryId, this.Supplier.SupplierId,
                this.UnitCost, this.RetailPrice, this.UnitsInStock,
                this.ReorderQuantity, this.LastOrderDate,
                this.NextOrderDate);
        }
    }
}
