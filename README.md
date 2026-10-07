# Databricks CDC Pipeline: E-Commerce Orders (Lakeflow Declarative Pipelines)

A hands-on Change Data Capture (CDC) project on Databricks. It syncs an operational `orders` table into a Lakehouse using **Auto Loader**, **Auto CDC (`create_auto_cdc_flow`)** and the **Medallion architecture**, with CDC events simulated as JSON files in a Unity Catalog Volume.

---

## 1. Business Background

**Company:** ShopKart, an online retail store.

The operational database serves live orders. Heavy analytics on it slows the website, so the analytics team needs a fresh copy of orders in the Lakehouse. A full reload is slow and costly, so we process **only what changed** (insert, update, delete).

## 2. Business Requirements

| # | Requirement | How it is met |
|---|---|---|
| R1 | New orders appear in the Lakehouse | `I` (insert) events |
| R2 | Status changes update the existing order, no duplicates | `U` (update) events, SCD Type 1 |
| R3 | Cancelled orders disappear from the current view | `D` (delete) events via `apply_as_deletes` |
| R4 | Late / out-of-order events must not overwrite newer data | `sequence_by = change_timestamp` |
| R5 | Raw events kept for audit and replay | Bronze is append-only |
| R6 | Daily sales report | Gold materialized view |
| R7 | Bad data is rejected | Data quality expectations |

## 3. Architecture

```
Volume (JSON files)  /Volumes/workspace/default/assets/CDC_DATA/
        │  Auto Loader (cloudFiles)
        ▼
BRONZE  orders_bronze     all raw CDC events + ingestion metadata
        │  create_auto_cdc_flow (SCD Type 1, apply_as_deletes)
        ▼
SILVER  order_silver      latest state per order_id
        │  aggregation
        ▼
GOLD    gold_table        daily summary (materialized view)
```

## 4. Source Data

### Columns

| Column | Type | Description |
|---|---|---|
| order_id | INT | Primary key |
| customer_id | INT | Customer |
| product_name | STRING | Item ordered |
| quantity | INT | Units |
| amount | DOUBLE | Order value |
| order_status | STRING | PLACED / SHIPPED / DELIVERED |
| city | STRING | Delivery city |
| updated_at | TIMESTAMP | Last modification in source |
| **operation** | STRING | CDC flag: `I` insert, `U` update, `D` delete |
| **change_timestamp** | TIMESTAMP | When the change happened in source (used for ordering) |

### Files

| File | Day | Content |
|---|---|---|
| `orders_batch_1.json` | Day 1 | Initial inserts: orders 101-104 |
| `orders_batch_2.json` | Day 2 | Updates 101, 102 to SHIPPED; insert 105; **delete 104** |
| `orders_batch_3.json` | Day 3 | 101 to DELIVERED; 103 to SHIPPED; **late event** for 101; **bad row** 106 |

Sample files are in the `data/` folder (JSON Lines: one JSON object per line).

## 5. Pipeline Logic

### Bronze: `orders_bronze`
- Reads JSON with Auto Loader (`cloudFiles`), incrementally, so each file is processed once.
- Adds audit columns: `ingestion_date`, `_source_file_path`, `_source_file_name`, `_source_file_offset`, `_source_file_timestamp`.
- Stores **every** event as-is.

### Silver: `order_silver`
Created with `create_auto_cdc_flow`:

| Parameter | Value | Purpose |
|---|---|---|
| `keys` | `["order_id"]` | Which row an event refers to |
| `sequence_by` | `change_timestamp` | Decides which event is newer; ignores stale events |
| `apply_as_deletes` | `operation = 'D'` | Defines what counts as a delete |
| `stored_as_scd_type` | `1` | Keep only the latest state, no history |

Data quality expectations (`expect_all_or_drop`):

| Rule | Expression |
|---|---|
| valid_amount | `amount > 0` |
| valid_operation | `operation IN ('I','U','D')` |

### Gold: `gold_table`
One row per date with `total_orders`, `total_amount`, `delivered_status_count`, `pending_status_count` (pending = anything not DELIVERED).

## 6. All Cases Covered

| # | Case | Example | Expected behaviour |
|---|---|---|---|
| 1 | **Insert** | Order 101, 102, 103, 104 (batch 1) | New row in Silver |
| 2 | **Update** | 101 and 102: PLACED to SHIPPED (batch 2) | Existing row updated, no duplicate |
| 3 | **Delete** | Order 104 (batch 2, `D`) | Row removed from Silver; the `D` event is applied, not stored |
| 4 | **Multiple updates, same order** | 101: I, then U (SHIPPED), then U (DELIVERED) | Final status DELIVERED |
| 5 | **Late / out-of-order event** | 101 SHIPPED (10-02) arrives after DELIVERED (10-03) | Ignored, because its `change_timestamp` is older |
| 6 | **Invalid data** | Order 106 with `amount = -900` | Dropped by `valid_amount` |
| 7 | **Insert in a later batch** | Order 105 (batch 2) | Added alongside existing rows |
| 8 | **Re-run with no new files** | Run the pipeline again | No change (Auto Loader checkpoint) |
| 9 | **Raw audit** | Query Bronze | All 12 events preserved, including the `D` row and order 106 |

### Why `D` events must reach the CDC flow
Order 104 is already in Silver from batch 1. Only the `D` event can remove it. Filtering `D` out (for example `operation IN ('I','U')`) leaves a cancelled order in Silver with no error. Two things are needed:
1. Allow `D` through the expectation.
2. Set `apply_as_deletes` so the flow knows what a delete looks like.

## 7. Expected Results After All 3 Batches

### Silver (`order_silver`)

| order_id | product_name | order_status | amount |
|---|---|---|---|
| 101 | Laptop | DELIVERED | 55000 |
| 102 | Phone | SHIPPED | 40000 |
| 103 | Headphones | SHIPPED | 3000 |
| 105 | Monitor | PLACED | 12000 |

Order 104 is deleted and order 106 is rejected.

### Gold (`gold_table`), as currently written (grouped by last change date)

| date | total_orders | total_amount | delivered_status_count | pending_status_count |
|---|---|---|---|---|
| 2026-10-02 | 2 | 52000 | 0 | 2 |
| 2026-10-03 | 2 | 58000 | 1 | 1 |

> Orders move between dates as they change status, because the date comes from `change_timestamp`. See Known Limitations.

## 8. How to Run

1. Create a Unity Catalog Volume and folder, e.g. `/Volumes/workspace/default/assets/CDC_DATA/`.
2. Upload **only** `orders_batch_1.json`.
3. Create a Lakeflow Declarative Pipeline and add `bronze.py`, `silver.py`, `gold.py` from `src/`.
4. Run it. Silver should have 4 orders.
5. Upload `orders_batch_2.json`, run again. Order 104 should vanish; 101 and 102 become SHIPPED.
6. Upload `orders_batch_3.json`, run again. 101 becomes DELIVERED; the late event is ignored; 106 is dropped.
7. Check the pipeline's **Data quality** tab for dropped rows.

Verification queries:

```sql
SELECT * FROM order_silver ORDER BY order_id;
SELECT * FROM order_silver WHERE order_id = 104;   -- expect 0 rows
SELECT * FROM gold_table ORDER BY date;
SELECT operation, COUNT(*) FROM orders_bronze GROUP BY operation;
```

## 9. Repository Structure

```
databricks-cdc-orders-pipeline/
├── README.md
├── data/
│   ├── orders_batch_1.json
│   ├── orders_batch_2.json
│   └── orders_batch_3.json
└── src/
    ├── bronze.py
    ├── silver.py
    └── gold.py
```

## 10. Known Limitations and Improvements

- **Gold date:** the report groups by `change_timestamp`, i.e. "last activity date". For true daily sales add an immutable `order_date` to the source and group by it.
- **JSON option:** `multiline` is meant for JSON arrays / pretty-printed files. For JSON Lines files it is not needed.
- **Column types:** Auto Loader infers JSON fields as strings by default. Use `cloudFiles.inferColumnTypes` or explicit casts for `amount` and `change_timestamp`.
- **Silver schema:** the `operation` column is stored in Silver. Use `except_column_list=["operation"]` to exclude it.
- **Delete rows and `amount`:** `amount > 0` also applies to `D` rows. Real CDC tools may send null values on deletes, so consider `operation = 'D' OR amount > 0`.
- **Possible extensions:** SCD Type 2 history, `shipped_status_count`, a city-level Gold table, Unity Catalog permissions.

## 11. Tech Stack

Databricks, Lakeflow Declarative Pipelines (`pyspark.pipelines`), Auto Loader, Delta Lake, Unity Catalog Volumes, PySpark.
