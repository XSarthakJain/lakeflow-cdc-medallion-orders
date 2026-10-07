from pyspark import pipelines as dp
from pyspark.sql.functions import col,current_timestamp,expr



dp.create_streaming_table(name="order_silver", expect_all_or_drop={"valid_amount": "amount > 0", "valid_operation": "operation IN ('I', 'U','D')"})

dp.create_auto_cdc_flow(
    target = "order_silver",
    source = "orders_bronze",
    keys = ["order_id"],
    apply_as_deletes = expr("operation = 'D'"),#It would delete the record if the operation is D not only D record along with that previous order_id records too
    sequence_by=col("change_timestamp"),
    stored_as_scd_type="1"
)