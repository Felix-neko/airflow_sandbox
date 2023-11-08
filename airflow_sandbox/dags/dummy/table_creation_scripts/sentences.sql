DROP TABLE IF EXISTS {{ full_table_name }};
CREATE TABLE {{ full_table_name }}(sentence_id INTEGER, sinner_id INTEGER, sentence STRING)
    PARTITIONED BY (dt STRING) STORED AS PARQUET;
