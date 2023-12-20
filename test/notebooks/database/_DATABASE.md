# Database

## Purpose

First, this database generator is for generating nearly real-world data.

The generated data is for

- testing ETL-Pipelines
- data modelling
- large data analysis
- machine learning testing
- trying to generate a automatic way for matching data across different datasources => MasterDataManagement

## Base Masterdata

This is a dataset, where the datasources get theire data. In The Datasources this masterdata will random sampled.

1. **client**

    - id (unique)
    - name
    - address (all Musterstraße {rng}, 10+{rng} Berlin)

2. **employee**

    - id (for mdm identification)
    - firstname
    - lastname
    - birthdate
    - entry_date
    - leave_date

3. **department**

    - name (2 samples each, semicolon seperated)

4. **costcenter**

    - buKr
    - kst_short
    - kst_long
    - name (2 samples each, semicolon seperated)

5. **project**

    - name (2 samples each, semicolon seperated)
    - description

6. **task**

    - name (2 samples each, semicolon seperated)
    - description

7. **businesspartner**

    - name (2 samples each, semicolon seperated)
    - address (all Musterstraße {rng}, 10+{rng} Berlin)

## Data and Datasources

### ERP-System - datev

**Description** <br>

ERP System with master data and transactional data. No Data deletion.


1. **dbo.client** Clients / Companies masterdata.

    - id (unique)
    - name
    - address
    - change_field (changing)

2. **dbo.employee** Employee masterdata.

    - id (unique)
    - client_id
    - firstname
    - lastname
    - birthdate
    - entry_date
    - leave_date (changing)
    - salary
    - change_field (changing)

3. **dbo.department** Department masterdata.

    - id (unique)
    - client_id
    - name (changing)
    - change_field (changing)

4. **dbo.costcenter** Costcenter masterdata.

    - id (unique)
    - kst
    - client_id
    - name (changing)

5. **dbo.paytype** Paymenttype

    - id (unique)
    - name ["AG-Anteil", "Lohnsteuer", "Soli-Zuschlag", "Krankenk.", "Pflegevers.", "Arbeitslosenvers.", "Rentenvers."]

6. **dbo.employee_pay** Employee payment transaction per month, one row per pay_type!

    - id
    - transaction_date
    - client_id
    - costcenter_id
    - pay_type_id
    - amount
    - [col1 - col100, for a very wide table, with 50 columns of number and 50 columns of text(50) values.]

### Projectmanagement System - ProjectConsultingTool

1. **dbo.users** masterdata

    - id
    - fullname
    - is_active (changing, 0|1)
    - creation_datetime
    - update_datetime

2. **dbo.project** masterdata

    - id
    - name
    - projectlead_id (changing, random user_id)
    - description (changing description)
    - creation_datetime
    - update_datetime

3. **dbo.department** masterdata

    - creation_datetime
    - update_datetime

4. **dbo.task** masterdata

    - creation_datetime
    - update_datetime

5. **dbo.project_user** masterdata

    - creation_datetime
    - update_datetime

6. **dbo.project_task** masterdata

    - creation_datetime
    - update_datetime

7. **dbo.businesspartner** masterdata

    - creation_datetime
    - update_datetime

8. **dbo.projecttime** projecttimes by user, project and task

    - 
    - [col1 - col100, for a very wide table, with 50 columns of number and 50 columns of text(50) values.]
    - creation_datetime
    - update_datetime

9. **mdm.employee** masterdata

### ECM-System - enaio

**Description** <br>

ECM System with highly changing data structures. A lot data deletion.

1. **tbl**

### HR-System - atoss [Not in dev]

System for managing the company employees with multiple clients.

1. **dbo.employee** Employee masterdata.

    - id (unique)
    - client_id (changing, random client_id)
    - firstname
    - lastname
    - birthdate
    - entry_date
    - leave_date (changing, only once)
    - salary (changing, random number)

2. **dbo.department** Department masterdata.

    - id (unique)
    - client_id
    - name (changing)

3. **dbo.costcenter** Costcenter masterdata.

    - id (unique)
    - kst
    - client_id
    - name (changing)

4. **dbo.client** Clients / Companies masterdata.

    - id (unique)
    - name (changing)
    - address (all musterstraße 7, 13663 Berlin)
