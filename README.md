# Airflow Best Practices: ETL DAGs

Two Apache Airflow DAG implementations demonstrating practical data-engineering and workflow-orchestration best practices, including **DAG scheduling, Jinja templating, Airflow Variables, XComs, Task Groups, task dependencies, SQL-to-S3 data movement, and data cleaning with Pandas**.

This project was completed as part of **Course 2 of the DeepLearning.AI Data Engineering Professional Certificate on Coursera**, in the **Airflow 101 – Best Practices** lab.

---

## 📌 Project Overview

This project demonstrates how Apache Airflow can be used to orchestrate an ETL pipeline that:

1. Extracts data from a MySQL database.
2. Loads the raw data into Amazon S3.
3. Cleans the extracted data by removing missing values and duplicate records.
4. Stores the transformed data in a separate S3 location.
5. Uses XComs to communicate the number of valid records between tasks.
6. Organizes related tasks using Airflow Task Groups.
7. Uses Airflow Variables and templating to avoid hard-coded configuration and dynamically generate date-based S3 paths.

The project contains two DAGs:

* `simple_dag.py` — A single-table ETL pipeline for the `orders` table.
* `grouped_tasks_dag.py` — A scalable Task Group-based ETL pipeline for the `payments`, `customers`, and `products` tables.

The lab's objective is to apply Airflow best practices while building deterministic and idempotent data pipelines.

---

## 🏗️ Architecture

### Simple DAG

```text
                    ┌─────────────────┐
                    │      Start      │
                    └────────┬────────┘
                             │
                             ▼
                 ┌───────────────────────┐
                 │ Extract & Load Orders │
                 │      MySQL → S3       │
                 └───────────┬───────────┘
                             │
                             ▼
                 ┌───────────────────────┐
                 │ Transform Orders      │
                 │  - Remove NULLs       │
                 │  - Remove duplicates  │
                 └───────────┬───────────┘
                             │
                             │ XCom
                             ▼
                 ┌───────────────────────┐
                 │     Notification      │
                 │ Valid record count    │
                 └───────────┬───────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │       End       │
                    └─────────────────┘
```

### Grouped Tasks DAG

```text
                         ┌─────────────┐
                         │    Start    │
                         └──────┬──────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
              ▼                 ▼                 ▼
        ┌───────────┐     ┌───────────┐     ┌───────────┐
        │ Payments  │     │ Customers │     │ Products  │
        │  Group    │     │   Group   │     │   Group   │
        └─────┬─────┘     └─────┬─────┘     └─────┬─────┘
              │                 │                 │
              ▼                 ▼                 ▼
        ┌───────────┐     ┌───────────┐     ┌───────────┐
        │ Extract & │     │ Extract & │     │ Extract & │
        │   Load    │     │   Load    │     │   Load    │
        └─────┬─────┘     └─────┬─────┘     └─────┬─────┘
              │                 │                 │
              ▼                 ▼                 ▼
        ┌───────────┐     ┌───────────┐     ┌───────────┐
        │Transform  │     │Transform  │     │Transform  │
        └─────┬─────┘     └─────┬─────┘     └─────┬─────┘
              │                 │                 │
              └─────────────────┼─────────────────┘
                                ▼
                     ┌────────────────────┐
                     │    Notification    │
                     └──────────┬─────────┘
                                │
                                ▼
                         ┌─────────────┐
                         │     End     │
                         └─────────────┘
```

The second DAG uses Airflow Task Groups to organize the extract and transform tasks for multiple tables, making the DAG easier to read and monitor.

---

## 🛠️ Technologies Used

| Technology            | Purpose                                   |
| --------------------- | ----------------------------------------- |
| **Python**            | DAG and transformation implementation     |
| **Apache Airflow**    | Workflow orchestration                    |
| **MySQL**             | Source relational database                |
| **Amazon RDS**        | Hosted MySQL source database              |
| **Amazon S3**         | Data storage for raw and transformed data |
| **Pandas**            | Data cleaning and transformation          |
| **Jinja Templates**   | Dynamic execution-date-based paths        |
| **Airflow Variables** | External configuration                    |
| **Airflow XComs**     | Communication between tasks               |
| **Task Groups**       | Organizing related Airflow tasks          |

The original lab uses a MySQL `classicmodels` database hosted on Amazon RDS and uses Amazon S3 for the data-storage layer.

---

## 📂 Repository Structure

```text
airflow-best-practices-dags/
│
├── simple_dag.py
├── grouped_tasks_dag.py
└── README.md
```

### `simple_dag.py`

Implements an ETL pipeline for the `orders` table.

Main workflow:

```text
MySQL orders
     │
     ▼
SqlToS3Operator
     │
     ▼
S3 Bronze
     │
     ▼
Pandas transformation
     │
     ├── Drop missing values
     └── Drop duplicates
     │
     ▼
S3 Silver
     │
     ▼
XCom: valid record count
     │
     ▼
Notification
```

The simple DAG contains five main tasks: start, extract/load, transform, notification, and end.

### `grouped_tasks_dag.py`

Extends the same ETL pattern to multiple tables:

```text
payments
customers
products
```

Each table receives its own Task Group containing:

```text
extract_load_<table>
        │
        ▼
transform_<table>
```

The groups then converge on the notification task before the DAG finishes.

The lab specifies this grouped approach for the `payments`, `customers`, and `products` tables.

---

# 🔑 Airflow Concepts Demonstrated

## 1. DAG Scheduling

The DAG is configured to execute on a daily schedule:

```python
schedule="@daily"
```

A static `start_date` and `catchup=False` are also used.

These settings help make DAG execution predictable and prevent unwanted historical runs.

---

## 2. Airflow Templating

The DAG dynamically generates a partition date using Airflow's built-in `ds` variable and the `ds_format` macro:

```python
partition_date = (
    '{{ macros.ds_format(ds, "%Y-%m-%d", "%Y/%m/%d") }}'
)
```

This allows S3 paths to be organized according to the DAG execution date rather than using a hard-coded date.

For example:

```text
bronze/2026/09/20/orders.csv
silver/2026/09/20/orders.csv
```

Airflow templating uses Jinja syntax and allows runtime information such as the DAG's logical date to be incorporated into task parameters.

---

## 3. Airflow Variables

The S3 bucket name is retrieved using an Airflow Variable:

```python
Variable.get("s3_bucket")
```

Instead of hard-coding the bucket name throughout the DAG, configuration is stored externally in Airflow.

This follows the DRY principle and makes the DAG easier to configure and maintain.

---

## 4. SQL-to-S3 Data Extraction

The project uses:

```python
SqlToS3Operator
```

to extract data from MySQL and write it to Amazon S3.

For the simple DAG, the SQL query is:

```sql
SELECT * FROM orders;
```

The data is initially stored in the **bronze** layer.

---

## 5. Data Cleaning with Pandas

The transformation task uses Pandas to perform basic data cleaning:

```python
df = df.dropna()
df = df.drop_duplicates()
```

This removes:

* rows containing missing values
* duplicate rows

The cleaned dataset is then written to the **silver** layer.

---

## 6. XCom Communication

The transformation task calculates the number of valid records:

```python
num_valid_records = len(df)
```

It then pushes the result to Airflow's XCom system:

```python
context["ti"].xcom_push(
    key="valid_records",
    value=num_valid_records
)
```

The notification task retrieves the value using:

```python
context["ti"].xcom_pull(
    task_ids=task_id,
    key="valid_records"
)
```

This demonstrates how Airflow tasks can exchange small pieces of metadata without directly depending on each other's Python execution context.

XComs are intended for relatively small pieces of information rather than large datasets because they are stored in the Airflow metadata database by default.

---

## 7. Task Groups

The grouped DAG uses:

```python
with TaskGroup(table) as etl_tg:
```

to organize related tasks.

Each table has an independent extract/load → transform workflow:

```text
payments
├── extract_load_payments
└── transform_payments

customers
├── extract_load_customers
└── transform_customers

products
├── extract_load_products
└── transform_products
```

The individual groups can then be connected to the rest of the DAG:

```python
start_task >> task_group >> notification_task >> end_task
```

Task Groups improve DAG readability and monitoring by grouping related tasks together in the Airflow UI.

---

# 🔄 Data Flow

The overall data flow is:

```text
                    MySQL / Amazon RDS
                           │
                           │
                     SQL SELECT
                           │
                           ▼
                  ┌─────────────────┐
                  │ Apache Airflow  │
                  │  Orchestration  │
                  └────────┬────────┘
                           │
                           ▼
                    Amazon S3 Bronze
                           │
                           │
                    Data Cleaning
                           │
                 ┌─────────┴─────────┐
                 │                   │
              Drop NULLs        Drop duplicates
                 │                   │
                 └─────────┬─────────┘
                           │
                           ▼
                    Amazon S3 Silver
                           │
                           ▼
                    Valid Record Count
                           │
                         XCom
                           │
                           ▼
                      Notification
```

---

# 📊 Bronze and Silver Layers

The pipeline separates extracted and transformed data into two storage zones.

### Bronze

Contains the data extracted from the source database.

```text
s3://<bucket>/bronze/<partition-date>/<table>.csv
```

### Silver

Contains the cleaned data after transformation.

```text
s3://<bucket>/silver/<partition-date>/<table>.csv
```

This separation provides a simple example of organizing raw and processed data within a data pipeline.

---

# 🔗 Task Dependencies

The simple DAG follows a linear dependency:

```python
start_task \
    >> extract_and_load_task \
    >> transform_task \
    >> notification_task \
    >> end_task
```

The grouped DAG introduces parallel table-level processing:

```python
start_task
    >> task_group
    >> notification_task
    >> end_task
```

Inside each Task Group:

```python
extract_load_task >> transform_task
```

This allows the extraction and transformation workflows for the different tables to be represented independently while sharing the same overall DAG structure.

---

# 🎯 Key Learning Outcomes

Through this project, I practiced:

* Building Apache Airflow DAGs with Python
* Scheduling DAGs with `@daily`
* Using static DAG start dates
* Controlling historical execution with `catchup=False`
* Using Airflow's built-in execution-date variables
* Applying Jinja templating
* Using Airflow macros
* Managing configuration with Airflow Variables
* Using `SqlToS3Operator`
* Defining task dependencies with `>>`
* Implementing transformations with `PythonOperator`
* Cleaning data with Pandas
* Passing metadata between tasks with XComs
* Creating reusable Task Groups
* Designing multi-table ETL workflows
* Organizing data into bronze and silver storage layers
* Applying concepts of deterministic and idempotent pipeline design

The lab explicitly identifies determinism, idempotence, templating, variables, XComs, and Task Groups as the core Airflow practices being applied.

---

# ⚠️ Production Considerations

This project is primarily an educational demonstration of Airflow orchestration concepts.

One important limitation is that the transformation is performed directly with Pandas inside an Airflow task.

For production-scale data pipelines, Airflow is generally better used as the **orchestration layer**, while computational workloads are delegated to appropriate processing systems such as databases, Spark, or other processing services.

The original lab explicitly notes that performing the Pandas transformation inside Airflow is done for educational purposes and is not considered the preferred production architecture.

A production implementation could therefore evolve toward:

```text
                 Apache Airflow
                /      |       \
               /       |        \
              ▼        ▼         ▼
           AWS S3   Database   Spark/ETL
```

with Airflow coordinating the workloads rather than performing large-scale transformations itself.

---

# 📚 Learning Context

This project was completed as part of:

**DeepLearning.AI Data Engineering Professional Certificate — Course 2**

Lab:

**Airflow 101 – Best Practices**

The original exercise focuses on applying Airflow best practices while creating both a simple DAG and a grouped-task DAG.

---

# 👨‍💻 Author

**Heinn Htet Zan**

Computer Science | Data Engineering | Software Development

* GitHub: [Koheinn](https://github.com/Koheinn)
* LinkedIn: [Heinn Htet Zan](https://www.linkedin.com/in/heinn-htet-zan/)

---

# 📄 License

This repository contains my completed Python implementations from an educational data-engineering lab.

The project is intended for **learning, portfolio demonstration, and reference purposes**.
