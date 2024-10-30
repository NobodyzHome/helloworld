CREATE EXTERNAL CATALOG my_paimon_catalog
PROPERTIES
(
    "type" = "paimon",
    'paimon.catalog.type'='filesystem',
    'paimon.catalog.warehouse'='hdfs://namenode:9000/my-paimon'
);

set catalog my_paimon_catalog;

show databases ;

explain analyze  select * from app.paimon_append;