# **Naming Conventions**

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

## **General Principles**

- **Naming Conventions**: Use snake_case, with lowercase letters and underscores (`_`) to separate words.
- **Language**: Use English for all names.
- **Avoid Reserved Words**: Do not use SQL reserved words as object names.

## **Table Naming Conventions**

### **Bronze Rules and Silver Rules**
- All names must start with the source system name, and table names must match their original names without renaming.
- **`<sourcesystem>_<entity>`**  
  - `<sourcesystem>`: Name of the source api (e.g., `players`, `teams`).  
  - `<entity>`: Exact table name from the source system.  
  - Example: `players_info` → Players information from the players API.

### **Gold Rules**
- All names must use meaningful, business-aligned names for tables, starting with the category prefix.
- **`<category>_<entity>`**  
  - `<category>`: Describes the role of the table, such as `dim` (dimension) or `fact` (fact table).  
  - `<entity>`: Descriptive name of the table, aligned with the business domain (e.g., `customers`, `products`, `sales`).  
  - Examples:
    - `dim_customers` → Dimension table for customer data.  
    - `fact_sales` → Fact table containing sales transactions.  

#### **Glossary of Category Patterns**

| Pattern     | Meaning                           | Example(s)                              |
|-------------|-----------------------------------|-----------------------------------------|
| `v_dimension_`      | Dimension table             | `v_dimension_players`, `v_dimenson_teams`   |
| `v_fct_`     | Fact results                       | `v_fact_results`                            |

## **Column Naming Conventions**

## **Surrogate Keys**  
- All primary keys in dimension tables must use the suffix `_key`.
- **`<table_name>_key`**  
  - `<table_name>`: Refers to the name of the table or entity the key belongs to.  
  - `_key`: A suffix indicating that this column is a surrogate key.  
  - Example: `players_key` → Surrogate key in the `v_dimension_players` table.
  
## **Technical Columns**
- All technical columns must start with the prefix `dwh_`, followed by a descriptive name indicating the column's purpose.
- **`dwh_<column_name>`**  
  - `dwh`: Prefix exclusively for system-generated metadata.  
  - `<column_name>`: Descriptive name indicating the column's purpose.  
  - Example: `dwh_ingestion_date` → System-generated column used to store the date when the record was loaded.

  NOTE while performing joint in the silver layer, use dwh_team_id, and dwh_player_id, This columns stay unique throughout in their respective tables
 
## **Stored Procedure**

- All stored procedures used for loading data must follow the naming pattern:
- **`schema.usp_update_<table_name>`**.
  
  - `<table_name>`: Represents the table being updated in the silver layer.
  - Example: 
    - `silver.usp_update_players_info()` → Stored procedure for updating the silver layer player info table with the new records.


**Bronze Layer:** Updating silver layer is handle by sqlalchemy.Data is truncated before added to any bronze table.
**Silver Layer:** Each stored procedure incrementally loads data based on the available and new data in bronze.
**Gold Layer:** Gold layer is fully virtual, view read data directly from the silver layer without creating new copies.