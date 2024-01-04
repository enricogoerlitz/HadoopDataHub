# HadoopDataHub Transformation Tool

## Technical Requirements

- Python < 3.12 & > 3.8 (recomended: 3.11.x)

## Installation

- conda create --name hdmh python=3.11


## Transformation Logic

**How to make an ETL with historized Data with Python on Hadoop Cluster**

1. Fullload into /hive/tmp/* (foreach batch)
    1. add columns
        - GUID
        - PK (concat with ||)
        - ROW_IS_CURRENT = 1
        - ROW_VALID_FROM = TODAY()

    2. save batch as file1..X.csv to /hive/tmp/*

2. DROP + CREATE EXTERNAL TABLE for /hive/tmp/*

3. ForEach Batch-File from /hive/tmp/*
    1. identify updated rows
        - Filter: JOIN ON PK, WHERE STG_COL1..X <> PSA_COL1..X
            - ROW_IS_CURRENT = 0
            - ROW_VALID_TO = TODAY()
            
    2. insert updated rows into /hive/tmp/* as updatedrecordfile1..X

4. Adding deleted rows to tmp
    1. Identify deleted rows
        - Filter: WHERE ROW_DELETE_DATETIME IS NULL
            - ==> insert deleted rows into /hive/tmp/*

        - Filter: psa.PK not in tmp.PK AND ROW_DELETE_DATETIME IS NOT NULL
            - limit(delete_batch_size) + foreach limit batch
            - loop untill filter returns 0 records
            - ROW_IS_CURRENT = 0
            - ROW_DELETE_DATETIME = ROW_DELETE_DATETIME || TODAY()
            - ==> insert deleted rows into /hive/tmp/*

5. Rename /hive/datahub/* to /hive/datahub_tmp_delete/*

6. Rename /hive/tmp/* to /hive/datahub/*

7. Delete /hive/datahub_tmp_delete/*

8. DROP + CREATE EXTERNAL TABLE for /hive/datahub/*


**Zusatz**

- typische Fact-Tables can be imported with just a fulload like 1.
- ALERT: Workaround, when source table as more or less columns then psa!
    - less: hold alt columns!
    - more: ...
- alles mit Pandas? Denke schon!


**TODOs**

- create a standard, param-controlled Python-ETL-Module for Hadoop-Cluster
    - historized=True -> Only 1., else 1. - 7.
- auch eine deltaload Klasse?