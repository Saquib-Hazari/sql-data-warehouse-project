# 📈 Data Catalog for gold layer.

---

## Overview

- The Gold Layer serves as the business-ready representation of data, designed specifically for analytical, reporting, and decision-making use cases. It consists of well-structured dimension and fact tables that provide a clear, meaningful, and optimized view of business metrics.

---

## 📌 Customer Dimension Table (`gold.dim_customer`)

The `dim_customer` table contains customer-related attributes that help in analytical and reporting use cases. The over all project is created using **Postgres in VS code**.

| Column Name       | Data Type     | Description                                                           |
| ----------------- | ------------- | --------------------------------------------------------------------- |
| `customer_key`    | `INT`         | Surrogate key uniquely identifying each customer record.              |
| `customer_id`     | `INT`         | Unique numerical identifier assigned to each customer.                |
| `customer_number` | `VARCHAR(50)` | Alphanumeric identifier representing the customer, used for tracking. |
| `first_name`      | `VARCHAR(50)` | The customer's first name, as recorded in the system.                 |
| `last_name`       | `VARCHAR(50)` | The customer's last name or family name.                              |
| `country`         | `VARCHAR(50)` | The country of residence for the customer (e.g., 'Australia').        |
| `marital_status`  | `VARCHAR(50)` | The marital status of the customer (e.g., 'Married', 'Single').       |
| `gender`          | `VARCHAR(50)` | The gender of the customer (e.g., 'Male', 'Female', 'n/a').           |
| `birthdate`       | `DATE`        | The date of birth of the customer (YYYY-MM-DD, e.g., 1971-10-06).     |
| `create_date`     | `DATE`        | The date when the customer record was created in the system.          |

## 📌 Product Dimension Table (`gold.dim_product`)

The `dim_product` table contains product-related attributes, providing a structured view of all products.

| Column Name            | Data Type     | Description                                                           |
| ---------------------- | ------------- | --------------------------------------------------------------------- |
| `product_key`          | `INT`         | Surrogate key uniquely identifying each product record.               |
| `product_id`           | `INT`         | A unique identifier assigned to the product for internal tracking.    |
| `product_number`       | `VARCHAR(50)` | Structured alphanumeric code representing the product.                |
| `product_name`         | `VARCHAR(50)` | Descriptive name of the product (e.g., type, color, size).            |
| `category_id`          | `VARCHAR(50)` | Unique identifier for the product's category.                         |
| `category`             | `VARCHAR(50)` | The broader classification of the product (e.g., Bikes, Components).  |
| `subcategory`          | `VARCHAR(50)` | A detailed classification within the category (e.g., Mountain Bikes). |
| `maintenance_required` | `VARCHAR(50)` | Indicates if the product requires maintenance (e.g., 'Yes', 'No').    |
| `cost`                 | `INT`         | The base price of the product in monetary units.                      |
| `product_line`         | `VARCHAR(50)` | The specific product line or series (e.g., Road, Mountain).           |
| `start_date`           | `DATE`        | The date when the product became available for sale.                  |

## 📌 Sales Fact Table (`gold.fact_sales`)

The `fact_sales` table captures transactional sales data, linking orders to products and customers.

| Column Name     | Data Type     | Description                                                |
| --------------- | ------------- | ---------------------------------------------------------- |
| `order_number`  | `VARCHAR(50)` | Unique alphanumeric identifier for each sales order.       |
| `product_key`   | `INT`         | Surrogate key linking to the product dimension.            |
| `customer_key`  | `INT`         | Surrogate key linking to the customer dimension.           |
| `order_date`    | `DATE`        | The date when the order was placed.                        |
| `shipping_date` | `DATE`        | The date when the order was shipped.                       |
| `due_date`      | `DATE`        | The date when the order payment was due.                   |
| `sales_amount`  | `INT`         | The total monetary value of the sale for the line item.    |
| `quantity`      | `INT`         | The number of product units ordered.                       |
| `price`         | `INT`         | The price per unit of the product in whole currency units. |
