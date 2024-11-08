CREATE MATERIALIZED VIEW mydb.mv_realtime_invocation_report
DISTRIBUTED BY HASH(`col_val`) BUCKETS 1
REFRESH ASYNC
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'theaterCode' col_field,
            theaterCode col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            theaterCode is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'waybillSource' col_field,
            waybillSource col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            waybillSource is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'deliveryType' col_field,
            deliveryType col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            deliveryType is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'siteName' col_field,
            siteName col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            siteName is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'deliveryThirdType' col_field,
            deliveryThirdType col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            deliveryThirdType is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'province_code' col_field,
            province_code col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            province_code is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'isExpress' col_field,
            isExpress col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            isExpress is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'productSubType' col_field,
            productSubType col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            productSubType is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'goodsType' col_field,
            goodsType col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            goodsType is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'isKa' col_field,
            isKa col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            isKa is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'areaCode' col_field,
            areaCode col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            areaCode is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'orgCode' col_field,
            orgCode col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            orgCode is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all


SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'partitionCode' col_field,
            partitionCode col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            partitionCode is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'deliverySubType' col_field,
            deliverySubType col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            deliverySubType is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'rejectionRoleId' col_field,
            rejectionRoleId col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            rejectionRoleId is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'isZy' col_field,
            isZy col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            isZy is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'productType' col_field,
            productType col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            productType is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'siteDimension' col_field,
            siteDimension col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            siteDimension is not null
    )
        t
GROUP BY
    col_field,
    col_val

union all

SELECT
    col_field,
    col_val,
    COUNT( *) col_cnt,
    any_value(all_cnt) all_cnt,
    ROUND(COUNT( *) / any_value(all_cnt) * 100, 2) col_rate,
    row_number() over(partition BY col_field order by COUNT( *) / any_value(all_cnt) DESC) col_rank
FROM
    (
        SELECT
            'waybillDimension' col_field,
            waybillDimension col_val,
            COUNT( *) over() all_cnt
        FROM
            mydb.realtime_delivery_invocation
        where
            theaterCode is not null
    )
        t
GROUP BY
    col_field,
    col_val;