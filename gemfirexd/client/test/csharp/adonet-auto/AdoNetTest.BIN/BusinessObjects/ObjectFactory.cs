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

namespace AdoNetTest.BIN.BusinessObjects
{
    /// <summary>
    /// Provides methods for creating new business objects with random data 
    /// for populating and modifying default database records
    /// </summary>
    class ObjectFactory
    {
        public static Object Create(ObjectType objType)
        {
            switch (objType)
            {
                case ObjectType.Address:
                    return CreateAddress();
                case ObjectType.Supplier:
                    return CreateSupplier();
                case ObjectType.Category:
                    return CreateCategory();
                case ObjectType.Product:
                    return CreateProduct();
                case ObjectType.Customer:
                    return CreateCustomer();
                case ObjectType.Order:
                    return CreateOrder();
                case ObjectType.OrderDetail:
                    return CreateOrderDetail();
                default:
                    return null;
            }
        }

        public static BusinessObject Update(BusinessObject obj)
        {
            if (obj is Address)
                return GetAddressInfo((Address)obj);
            else if (obj is Supplier)
                return GetSupplierInfo((Supplier)obj);
            else if (obj is Category)
                return GetCategoryInfo((Category)obj);
            else if (obj is Customer)
                return GetCustomerInfo((Customer)obj);
            else if (obj is Product)
                return GetProductInfo((Product)obj);
            else if (obj is Order)
                return GetOrderInfo((Order)obj);
            else if (obj is OrderDetail)
                return GetOrderDetailInfo((OrderDetail)obj);
            else
                return null;
        }

        # region Address

        private static Address CreateAddress()
        {
            Address address = new Address();
            address.AddressId = GetNewAddressId();

            return GetAddressInfo(address);
        }

        private static Address GetAddressInfo(Address address)
        {
            address.Address1 = GetAddress1();
            address.Address2 = GetAddress2();
            address.Address3 = GetAddress3();
            address.City = GetCity();
            address.State = GetState();
            address.ZipCode = GetZipCode();
            address.Province = GetProvince();
            address.CountryCode = GetCountryCode();

            return address;
        }

        private static long GetNewAddressId()
        {
            return DbHelper.GetLastRowId(TableName.ADDRESS.ToString(),
                DbDefault.GetTableStructure(TableName.ADDRESS).PKColumns[0]) + 1;
        }

        private static String GetAddress1()
        {
            return String.Format("{0} {1} st.",
                DbHelper.GetRandomNumber(4), DbHelper.GetRandomString(10));
        }

        private static String GetAddress2()
        {
            return DbHelper.GetRandomString(2);
        }

        private static String GetAddress3()
        {
            return DbHelper.GetRandomString(2);
        }

        private static String GetCity()
        {
            return DbHelper.GetRandomString(10);
        }

        private static StateCodes GetState()
        {
            return (StateCodes)(DbHelper.GetRandomNumber(1, 61));
        }

        private static String GetZipCode()
        {
            return DbHelper.GetRandomNumber(5).ToString();
        }

        private static String GetProvince()
        {
            return DbHelper.GetRandomString(2);
        }

        private static CountryCodes GetCountryCode()
        {
            return CountryCodes.UnitedStates;
        }

        # endregion

        # region Supplier

        private static Supplier CreateSupplier()
        {
            Supplier supplier = new Supplier();
            supplier.SupplierId = GetNewSupplierId();

            return GetSupplierInfo(supplier);
        }

        private static Supplier GetSupplierInfo(Supplier supplier)
        {
            supplier.Name = GetSupplierName();
            supplier.Address = GetSupplierAddress();
            supplier.Phone = GetSupplierPhone();
            supplier.Email = GetSupplierEmail();

            return supplier;
        }

        private static long GetNewSupplierId()
        {
            return DbHelper.GetLastRowId(TableName.SUPPLIER.ToString(),
                DbDefault.GetTableStructure(TableName.SUPPLIER).PKColumns[0]) + 1;
        }

        private static String GetSupplierName()
        {
            return DbHelper.GetRandomString(10);
        }

        private static Address GetSupplierAddress()
        {
            Address address = new Address();
            address.AddressId = DbHelper.GetRandomRowId(
                TableName.ADDRESS.ToString(), 
                DbDefault.GetTableStructure(TableName.ADDRESS).PKColumns[0]);

            return address;
        }

        private static String GetSupplierPhone()
        {
            return String.Format("({0}) {1}-{2}",
                DbHelper.GetRandomNumber(3), DbHelper.GetRandomNumber(3),
                DbHelper.GetRandomNumber(4));
        }

        private static String GetSupplierEmail()
        {
            return String.Format("{0}.{1}@{2}.com",
                DbHelper.GetRandomString(5), DbHelper.GetRandomString(10),
                DbHelper.GetRandomString(10));
        }

        # endregion

        # region Category

        private static Category CreateCategory()
        {
            Category category = new Category();
            category.CategoryId = GetNewCategoryId();

            return GetCategoryInfo(category);
        }

        private static Category GetCategoryInfo(Category category)
        {
            category.Name = GetCategoryName();
            category.Description = GetCategoryDescription();

            return category;
        }

        private static long GetNewCategoryId()
        {
            return DbHelper.GetLastRowId(TableName.CATEGORY.ToString(),
                DbDefault.GetTableStructure(TableName.CATEGORY).PKColumns[0]) + 1;
        }

        private static String GetCategoryName()
        {
            return "Category " + DbHelper.GetRandomString(20);
        }

        private static String GetCategoryDescription()
        {
            return "Category Description " + DbHelper.GetRandomString(100);
        }

        # endregion

        # region Product

        private static Product CreateProduct()
        {
            Product product = new Product();
            product.ProductId = GetNewProductId();

            return GetProductInfo(product);
        }

        private static Product GetProductInfo(Product product)
        {
            product.Name = GetProductName();
            product.Description = GetProductDescription();
            product.Category = GetProductCategory();
            product.Supplier = GetProductSupplier();
            product.UnitCost = GetProductUnitCost();
            product.RetailPrice = GetProductRetailPrice();
            product.UnitsInStock = GetProductUnitsInStock();
            product.ReorderQuantity = GetProductReorderQuantity();
            product.LastOrderDate = GetProductLastOrderDate();
            product.NextOrderDate = GetProductNextOrderDate();

            return product;
        }

        private static long GetNewProductId()
        {
            return DbHelper.GetLastRowId(TableName.PRODUCT.ToString(),
                DbDefault.GetTableStructure(TableName.PRODUCT).PKColumns[0]) + 1;
        }

        private static String GetProductName()
        {
            return "Product " + DbHelper.GetRandomString(25);
        }

        private static String GetProductDescription()
        {
            return "Product Description " + DbHelper.GetRandomString(100);
        }

        private static Category GetProductCategory()
        {
            Category category = new Category();
            category.CategoryId = DbHelper.GetRandomRowId(
                TableName.CATEGORY.ToString(), 
                DbDefault.GetTableStructure(TableName.CATEGORY).PKColumns[0]);

            return category;
        }

        private static Supplier GetProductSupplier()
        {
            Supplier supplier = new Supplier();
            supplier.SupplierId = DbHelper.GetRandomRowId(
                TableName.SUPPLIER.ToString(), 
                DbDefault.GetTableStructure(TableName.SUPPLIER).PKColumns[0]);

            return supplier;
        }

        private static float GetProductUnitCost()
        {
            return (float)(DbHelper.GetRandomNumber(3) + 0.95);
        }

        private static float GetProductRetailPrice()
        {
            return (float)(DbHelper.GetRandomNumber(4) + 0.99);
        }

        private static int GetProductUnitsInStock()
        {
            return DbHelper.GetRandomNumber(3);
        }

        private static int GetProductReorderQuantity()
        {
            return DbHelper.GetRandomNumber(2);
        }

        private static DateTime GetProductLastOrderDate()
        {
            return DateTime.Today;
        }

        private static DateTime GetProductNextOrderDate()
        {
            return DateTime.Today.AddMonths(3);
        }

        # endregion

        # region Customer

        private static Customer CreateCustomer()
        {
            Customer customer = new Customer();
            customer.CustomerId = GetNewCustomerId();

            return GetCustomerInfo(customer);
        }

        private static Customer GetCustomerInfo(Customer customer)
        {
            customer.FirstName = GetCustomerFirstName();
            customer.LastName = GetCustomerLastName();
            customer.Address = GetCustomerAddress();
            customer.Phone = GetCustomerPhone();
            customer.Email = GetCustomerEmail();
            customer.LastOrderDate = GetCustomerLastOrderDate();

            return customer;
        }

        private static long GetNewCustomerId()
        {
            return DbHelper.GetLastRowId(TableName.CUSTOMER.ToString(),
                DbDefault.GetTableStructure(TableName.CUSTOMER).PKColumns[0]) + 1;
        }

        private static String GetCustomerFirstName()
        {
            return DbHelper.GetRandomString(7);
        }

        private static String GetCustomerLastName()
        {
            return DbHelper.GetRandomString(10);
        }

        private static Address GetCustomerAddress()
        {
            Address address = new Address();
            address.AddressId = DbHelper.GetRandomRowId(
                TableName.ADDRESS.ToString(),
                DbDefault.GetTableStructure(TableName.ADDRESS).PKColumns[0]);

            return address;
        }

        private static String GetCustomerPhone()
        {
            return String.Format("({0}) {1}-{2}",
                DbHelper.GetRandomNumber(3), DbHelper.GetRandomNumber(3),
                DbHelper.GetRandomNumber(4));
        }

        private static String GetCustomerEmail()
        {
            return String.Format("{0}.{1}@{2}.com",
                DbHelper.GetRandomString(5), DbHelper.GetRandomString(10),
                DbHelper.GetRandomString(10));
        }

        private static DateTime GetCustomerLastOrderDate()
        {
            return DateTime.Today;
        }

        private static IList<Order> GetCustomerOrders()
        {
            return new List<Order>();
        }

        # endregion

        # region Order

        private static Order CreateOrder()
        {
            Order order = new Order();
            order.OrderId = GetNewOrderId();

            return GetOrderInfo(order);
        }

        private static Order GetOrderInfo(Order order)
        {
            order.OrderDate = GetOrderDate();
            order.ShipDate = GetOrderShipDate();
            order.Customer = GetOrderCustomer();
            order.OrderDetails = new List<OrderDetail>();
            order.SubTotal = GetOrderSubTotal();

            return order;
        }

        private static long GetNewOrderId()
        {
            return DbHelper.GetLastRowId(TableName.ORDERS.ToString(),
                DbDefault.GetTableStructure(TableName.ORDERS).PKColumns[0]) + 1;
        }

        private static DateTime GetOrderDate()
        {
            return DateTime.Today;
        }

        private static DateTime GetOrderShipDate()
        {
            return DateTime.Today.AddDays(5);
        }

        private static Customer GetOrderCustomer()
        {
            return new Customer();
        }

        private static IList<OrderDetail> GetOrderDetails()
        {
            return new List<OrderDetail>();
        }

        private static float GetOrderSubTotal()
        {
            return (float)0.00;
        }
        

        # endregion

        # region OrderDetail

        private static OrderDetail CreateOrderDetail()
        {
            OrderDetail ordDetail = new OrderDetail();
            ordDetail.OrderId = GetDetailOrderId();
            ordDetail.ProductId = GetDetailProductId();

            return GetOrderDetailInfo(ordDetail);            
        }

        private static OrderDetail GetOrderDetailInfo(OrderDetail ordDetail)
        {
            ordDetail.Quantity = GetDetailQuantity();
            ordDetail.UnitPrice = GetDetailUnitPrice();
            ordDetail.Discount = GetDetailDiscount();

            return ordDetail;
        }

        private static long GetDetailOrderId()
        {
            return 0;
        }

        private static long GetDetailProductId()
        {
            return 0;
        }

        private static int GetDetailQuantity()
        {
            return DbHelper.GetRandomNumber(2);
        }

        private static float GetDetailUnitPrice()
        {
            return (float)0.00;
        }

        private static float GetDetailDiscount()
        {
            return (float)0.00;
        }

        # endregion
    }
}
