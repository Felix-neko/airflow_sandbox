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



DROP TABLE IF EXISTS {{ dest.documents }};

CREATE TABLE {{ dest.documents }}
(
    folder_id INT,
    document_id STRING,
    name STRING,
    payload STRING
)
    PARTITIONED BY (dt STRING) STORED AS PARQUET;

INSERT INTO {{ dest.documents }} VALUES
(0, '0m', 'Music/Red hot chili peppers.mp3', NULL, '2021-01-01'),
(0, '1m', 'Music/Infected mushroom.mp3', NULL, '2021-01-01'),
(0, '2m', 'Music/2ch OST.flac', NULL, '2021-01-01'),
(1, '0v', 'Videos/Следствие вели.mkv', NULL, '2021-01-01'),
(1, '1v', 'Videos/Дорожный патруль.mkv', NULL, '2021-01-01'),
(1, '2v', 'Videos/Играй гармонь.mkv', NULL, '2021-01-01'),
(1, '3v', 'Videos/Деревня дураков.mkv', NULL, '2021-01-01'),
(2, '0m', 'Memes/Мемы с 4chan.zip', NULL, '2021-01-01'),
(2, '1m', 'Memes/Мемы с 2ch.hk.rar', NULL, '2021-01-01'),
(2, '2m', 'Memes/Мемы из Телеги.tar.gz', NULL, '2021-01-01')
;

DROP TABLE IF EXISTS {{ dest.documents }}_keys;

CREATE TABLE {{ dest.documents }}_keys
(
    folder_id INT,
    document_id STRING
);

INSERT INTO {{ dest.documents }}_keys VALUES
(0, '0m'),
(0, '1m'),
(0, '2m'),
(2, '2m')
;