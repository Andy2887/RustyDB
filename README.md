# RustyDB

A relational database management system implemented in Rust, featuring a complete SQL execution engine, buffer pool management, and storage layer.

## Overview

RustyDB is a educational database system that implements core database concepts including:
- SQL parsing and execution
- Buffer pool management with LRU-K replacement policy
- Disk-based storage with heap files
- Query planning and optimization
- Aggregate operations (COUNT, SUM, AVG, MIN, MAX)
- Table joins and filtering

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   SQL Engine    │    │  Query Planner  │    │   Executors     │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ • Parser        │───▶│ • Optimizer     │───▶│ • Aggregation   │
│ • Session Mgmt  │    │ • Plan Builder  │    │ • Joins         │
│ • Result Format │    │ • Expression    │    │ • Filtering     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Storage Engine                              │
├─────────────────┬─────────────────┬─────────────────────────────┤
│ Buffer Pool Mgr │  Disk Manager   │      Heap File Manager      │
├─────────────────┼─────────────────┼─────────────────────────────┤
│ • LRU-K Cache   │ • Page I/O      │ • Table Management          │
│ • Pin/Unpin     │ • File Mgmt     │ • Row Serialization         │
│ • Dirty Tracking│ • Allocation    │ • Record ID Management      │
└─────────────────┴─────────────────┴─────────────────────────────┘
```


## Features

### SQL Support
- **Data Definition Language (DDL)**: `CREATE TABLE`, `INSERT`
- **Data Query Language (DQL)**: `SELECT` with `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`
- **Joins**: Support for table joins with various conditions
- **Aggregations**: COUNT, SUM, AVG, MIN, MAX functions
- **Data Types**: INTEGER, FLOAT, BOOLEAN, STRING, NULL

### Storage Engine
- **Buffer Pool Manager**: Efficient page caching with configurable pool size
- **LRU-K Replacer**: Advanced page replacement algorithm for optimal cache performance
- **Disk Manager**: Persistent storage with page-based file management
- **Heap Files**: Organized storage for table data

### Query Processing
- **Parser**: Complete SQL parser built with custom lexer
- **Planner**: Query optimization and execution plan generation
- **Executor**: Efficient query execution with operator pipelining

## Acknowledgements

This project was developed as part of **CS339: Introduction to Database Systems** at **Northwestern University**. The implementation builds upon starter code provided by the course, with student contributions focusing on core database system components such as storage engine, query planner and executors.

