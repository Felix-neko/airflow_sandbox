{% set example_schema = settings['src_settings']['default_schema_name'] %}

USE {{ example_schema }};

DROP TABLE IF EXISTS {{ src.sinner_souls }};

CREATE TABLE {{ src.sinner_souls }}(id INTEGER,name STRING) PARTITIONED BY (dt STRING) STORED AS PARQUET;

INSERT INTO {{ src.sinner_souls }} PARTITION (dt="2021-01-01") VALUES
    (0, "'El Chapo; With; 'Quotes'; And; Semicolons'");

INSERT INTO {{ src.sinner_souls }} PARTITION (dt="1992-01-01") VALUES
    (1, "Don Pablo Emiio Escobar Gaviria Without Any Semicolon");


-- DROP TABLE IF EXISTS {{ dest.sentences }};
-- CREATE TABLE {{ dest.sentences }}(sentence_id INTEGER, sinner_id INTEGER, sentence STRING)
--     PARTITIONED BY (dt STRING) STORED AS PARQUET;
