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
using AdoNetTest.BIN.BusinessObjects;
using AdoNetTest.BIN.DataObjects;
using Pivotal.Data.GemFireXD;

namespace AdoNetTest.BIN
{
    /// <summary>
    /// Common facade for data object interfaces.
    /// </summary>
    class DbController
    {
        public GFXDClientConnection Connection { get; set; }
        public String SchemaName { get; set; }

        public DbController(GFXDClientConnection connection)
        {
            Connection = connection;
            SchemaName = String.Empty;
        }

        public DbController(GFXDClientConnection connection, String schemaName)
        {
            Connection = connection;
            SchemaName = schemaName;
        }

        public void ProcessNewCustomerOrder(IList<BusinessObject> bObjects)
        {
            foreach (BusinessObject bObject in bObjects)
            {
                if (bObject is Address)
                    AddAddress((Address)bObject);
                else if (bObject is Supplier)
                    AddSupplier((Supplier)bObject);
                else if (bObject is Category)
                    AddCategory((Category)bObject);
                else if (bObject is Customer)
                    AddCustomer((Customer)bObject);
                else if (bObject is Product)
                    AddProduct((Product)bObject);
                else if (bObject is Order)
                    AddOrder((Order)bObject);
                else if (bObject is OrderDetail)
                    AddOrderDetail((OrderDetail)bObject);
                else
                    throw new Exception("Unrecognized business object type");
            }
        }

        public bool ValidateTransaction(IList<BusinessObject> bObjects)
        {
            bool isValid = true;

            foreach (BusinessObject bObject in bObjects)
            {
                if (bObject is Address)
                    isValid = ValidateAddress(bObject);
                else if (bObject is Supplier)
                    isValid = ValidateSupplier(bObject);
                else if (bObject is Category)
                    isValid = ValidateCategory(bObject);
                else if (bObject is Customer)
                    isValid = ValidateCustomer(bObject);
                else if (bObject is Product)
                    isValid = ValidateProduct(bObject);
                else if (bObject is Order)
                    isValid = ValidateOrder(bObject);
                else if (bObject is OrderDetail)
                    isValid = ValidateOrderDetail(bObject);
                else
                    throw new Exception("Unrecognized business object type");
            }

            return isValid;
        }

        public String GetValidationErrors(IList<BusinessObject> bObjects)
        {
            StringBuilder errors = new StringBuilder();
            errors.Append("Transaction validation failed. Errors: ");

            foreach (BusinessObject bObject in bObjects)
            {
                foreach(String error in bObject.ValidationErrors)
                    errors.AppendFormat("Type: {0}, Error: {1}; ", bObject.GetType().Name, error);
            }

            return errors.ToString();
        }

        public bool ValidateAddress(BusinessObject bObject)
        {
            Address address = (Address)bObject;
            return address.Validate(GetAddress(address.AddressId));
        }

        public bool ValidateSupplier(BusinessObject bObject)
        {
            Supplier supplier = (Supplier)bObject;
            return supplier.Validate(GetSupplier(supplier.SupplierId));
        }

        public bool ValidateCategory(BusinessObject bObject)
        {
            Category category = (Category)bObject;
            return category.Validate(GetCategory(category.CategoryId));
        }

        public bool ValidateCustomer(BusinessObject bObject)
        {
            Customer customer = (Customer)bObject;
            return customer.Validate(GetCustomer(customer.CustomerId));
        }

        public bool ValidateProduct(BusinessObject bObject)
        {
            Product product = (Product)bObject;
            return product.Validate(GetProduct(product.ProductId));
        }

        public bool ValidateOrder(BusinessObject bObject)
        {
            Order order = (Order)bObject;
            return order.Validate(GetOrder(order.OrderId));
        }

        public bool ValidateOrderDetail(BusinessObject bObject)
        {
            OrderDetail ordDetail = (OrderDetail)bObject;
            return ordDetail.Validate(GetOrderDetail(ordDetail.OrderId, ordDetail.ProductId));
        }

        public long AddAddress(Address address)
        {
            return (new AddressDao(Connection, SchemaName)).Insert(address);
        }

        public long AddSupplier(Supplier supplier)
        {
            return (new SupplierDao(Connection, SchemaName)).Insert(supplier);
        }

        public long AddCategory(Category category)
        {
            return (new CategoryDao(Connection, SchemaName)).Insert(category);
        }

        public long AddProduct(Product product)
        {
            return (new ProductDao(Connection, SchemaName)).Insert(product);
        }

        public long AddCustomer(Customer customer)
        {
            return (new CustomerDao(Connection, SchemaName)).Insert(customer);
        }

        public long AddOrder(Order order)
        {
            return (new OrderDao(Connection, SchemaName)).Insert(order);
        }

        public long AddOrderDetail(OrderDetail ordDetail)
        {
            return (new OrderDetailDao(Connection, SchemaName)).Insert(ordDetail);
        }

        public long UpdateAddress(Address address)
        {
            return (new AddressDao(Connection, SchemaName)).Update(address);
        }

        public long UpdateSupplier(Supplier supplier)
        {
            return (new SupplierDao(Connection, SchemaName)).Update(supplier);
        }

        public long UpdateCategory(Category category)
        {
            return (new CategoryDao(Connection, SchemaName)).Update(category);
        }

        public long UpdateProduct(Product product)
        {
            return (new ProductDao(Connection, SchemaName)).Update(product);
        }

        public long UpdateCustomer(Customer customer)
        {
            return (new CustomerDao(Connection, SchemaName)).Update(customer);
        }

        public long UpdateOrder(Order order)
        {
            return (new OrderDao(Connection, SchemaName)).Update(order);
        }

        public long UpdateOrderDetail(OrderDetail ordDetail)
        {
            return (new OrderDetailDao(Connection, SchemaName)).Update(ordDetail);
        }

        public int DeleteAddress(long addressId)
        {
            return (new AddressDao(Connection, SchemaName)).Delete(addressId);
        }

        public int DeleteSupplier(long supplierId)
        {
            return (new SupplierDao(Connection, SchemaName)).Delete(supplierId);
        }

        public int DeleteCategory(long categoryId)
        {
            return (new CategoryDao(Connection, SchemaName)).Delete(categoryId);
        }

        public int DeleteProduct(long productId)
        {
            return (new ProductDao(Connection, SchemaName)).Delete(productId);
        }

        public int DeleteCustomer(long customerId)
        {
            return (new CustomerDao(Connection, SchemaName)).Delete(customerId);
        }

        public int DeleteOrder(long orderId)
        {
            return (new OrderDao(Connection, SchemaName)).Delete(orderId);
        }

        public int DeleteOrderDetail(long orderId, long productId)
        {
            return (new OrderDetailDao(Connection, SchemaName)).Delete(orderId, productId);
        }

        public Address GetAddress(long addressId)
        {
            return (new AddressDao(Connection, SchemaName)).Select(addressId);
        }

        public Supplier GetSupplier(long supplierId)
        {
            return (new SupplierDao(Connection, SchemaName)).Select(supplierId);
        }

        public Category GetCategory(long categoryId)
        {
            return (new CategoryDao(Connection, SchemaName)).Select(categoryId);
        }

        public Product GetProduct(long productId)
        {
            return (new ProductDao(Connection, SchemaName)).Select(productId);
        }

        public Customer GetCustomer(long customerId)
        {
            return (new CustomerDao(Connection, SchemaName)).Select(customerId);
        }

        public Order GetOrder(long orderId)
        {
            return (new OrderDao(Connection, SchemaName)).Select(orderId);
        }

        public IList<Order> GetOrdersByCustomer(long customerId)
        {
            return (new OrderDao(Connection, SchemaName)).SelectByCustomer(customerId);
        }

        public OrderDetail GetOrderDetail(long orderId, long productId)
        {
            return (new OrderDetailDao(Connection, SchemaName)).Select(orderId, productId);
        }

        public IList<Address> GetAddresses()
        {
            return (new AddressDao(Connection, SchemaName)).Select();
        }

        public IList<Supplier> GetSuppliers()
        {
            return (new SupplierDao(Connection, SchemaName)).Select();
        }

        public IList<Category> GetCategories()
        {
            return (new CategoryDao(Connection, SchemaName)).Select();
        }

        public IList<Product> GetProducts()
        {
            return (new ProductDao(Connection, SchemaName)).Select();
        }

        public IList<Customer> GetCustomers()
        {
            return (new CustomerDao(Connection, SchemaName)).Select();
        }

        public IList<Order> GetOrders()
        {
            return (new OrderDao(Connection, SchemaName)).Select();
        }

        public IList<OrderDetail> GetOrderDetails()
        {
            return (new OrderDetailDao(Connection, SchemaName)).Select();
        }

        public IList<OrderDetail> GetOrderDetails(long orderId)
        {
            return (new OrderDetailDao(Connection, SchemaName)).Select(orderId);
        }

        public Address GetRandomAddress()
        {
            return (new AddressDao(Connection, SchemaName)).SelectRandom();
        }
        public IList<Address> GetRandomAddresses(int numRecords)
        {
            return (new AddressDao(Connection, SchemaName)).SelectRandom(numRecords);
        }

        public Supplier GetRandomSupplier()
        {
            return (new SupplierDao(Connection, SchemaName)).SelectRandom();
        }

        public IList<Supplier> GetRandomSuppliers(int numRecords)
        {
            return (new SupplierDao(Connection, SchemaName)).SelectRandom(numRecords);
        }

        public Category GetRandomCategory()
        {
            return (new CategoryDao(Connection, SchemaName)).SelectRandom();
        }

        public IList<Category> GetRandomCategories(int numRecords)
        {
            return (new CategoryDao(Connection, SchemaName)).SelectRandom(numRecords);
        }

        public Product GetRandomProduct()
        {
            return (new ProductDao(Connection, SchemaName)).SelectRandom();
        }

        public IList<Product> GetRandomProducts(int numRecords)
        {
            return (new ProductDao(Connection, SchemaName)).SelectRandom(numRecords);
        }

        public Customer GetRandomCustomer()
        {
            return (new CustomerDao(Connection, SchemaName)).SelectRandom();
        }

        public IList<Customer> GetRandomCustomers(int numRecords)
        {
            return (new CustomerDao(Connection, SchemaName)).SelectRandom(numRecords);
        }

        public Order GetRandomOrder()
        {
            return (new OrderDao(Connection, SchemaName)).SelectRandom();
        }

        public IList<Order> GetRandomOrders(int numRecords)
        {
            return (new OrderDao(Connection, SchemaName)).SelectRandom(numRecords);
        }

        public OrderDetail GetRandomOrderDetail()
        {
            return (new OrderDetailDao(Connection, SchemaName)).SelectRandom();
        }

        public IList<OrderDetail> GetRandomOrderDetails(int numRecord)
        {
            return (new OrderDetailDao(Connection, SchemaName)).SelectRandom(numRecord);
        }

        public long GetAddressCount()
        {
            return (new AddressDao(Connection, SchemaName)).SelectCount();
        }

        public long GetSupplierCount()
        {
            return (new SupplierDao(Connection, SchemaName)).SelectCount();
        }

        public long GetCategoryCount()
        {
            return (new CategoryDao(Connection, SchemaName)).SelectCount();
        }

        public long GetProductCount()
        {
            return (new ProductDao(Connection, SchemaName)).SelectCount();
        }

        public long GetCustomerCount()
        {
            return (new CustomerDao(Connection, SchemaName)).SelectCount();
        }

        public long GetOrderCount()
        {
            return (new OrderDao(Connection, SchemaName)).SelectCount();
        }

        public long GetOrderDetailCount()
        {
            return (new OrderDetailDao(Connection, SchemaName)).SelectCount();
        }

        public int CreateView(Relation relation)
        {
            return GFXDDbi.Create(Connection, DbDefault.GetCreateViewStatement(relation));
        }

        public DataTable GetView(String viewName)
        {
            return (DataTable)GFXDDbi.Select(Connection, "SElECT * FROM " + viewName, QueryTypes.DATATABLE);
        }

        public int DropView(String viewName)
        {
            return GFXDDbi.Drop(Connection, "DROP VIEW " + viewName);
        }
    }
}
