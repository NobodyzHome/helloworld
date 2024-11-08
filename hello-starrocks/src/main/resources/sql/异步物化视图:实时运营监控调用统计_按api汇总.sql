CREATE MATERIALIZED VIEW mydb.mv_realtime_invocation_api_report
DISTRIBUTED BY HASH(`apiname`) BUCKETS 1
REFRESH ASYNC
PROPERTIES (
    "replication_num" = "3"
)
AS
SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'theaterCode' col_field,
                    theaterCode col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    theaterCode IS NOT NULL
                GROUP BY
                    apiname,
                    theaterCode
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'waybillSource' col_field,
                    waybillSource col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    waybillSource IS NOT NULL
                GROUP BY
                    apiname,
                    waybillSource
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'deliveryType' col_field,
                    deliveryType col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    deliveryType IS NOT NULL
                GROUP BY
                    apiname,
                    deliveryType
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'siteName' col_field,
                    siteName col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    siteName IS NOT NULL
                GROUP BY
                    apiname,
                    siteName
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'deliveryThirdType' col_field,
                    deliveryThirdType col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    deliveryThirdType IS NOT NULL
                GROUP BY
                    apiname,
                    deliveryThirdType
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'province_code' col_field,
                    province_code col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    province_code IS NOT NULL
                GROUP BY
                    apiname,
                    province_code
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'isExpress' col_field,
                    isExpress col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    isExpress IS NOT NULL
                GROUP BY
                    apiname,
                    isExpress
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'productSubType' col_field,
                    productSubType col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    productSubType IS NOT NULL
                GROUP BY
                    apiname,
                    productSubType
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'goodsType' col_field,
                    goodsType col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    goodsType IS NOT NULL
                GROUP BY
                    apiname,
                    goodsType
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'isKa' col_field,
                    isKa col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    isKa IS NOT NULL
                GROUP BY
                    apiname,
                    isKa
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'areaCode' col_field,
                    areaCode col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    areaCode IS NOT NULL
                GROUP BY
                    apiname,
                    areaCode
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'orgCode' col_field,
                    orgCode col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    orgCode IS NOT NULL
                GROUP BY
                    apiname,
                    orgCode
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'partitionCode' col_field,
                    partitionCode col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    partitionCode IS NOT NULL
                GROUP BY
                    apiname,
                    partitionCode
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'deliverySubType' col_field,
                    deliverySubType col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    deliverySubType IS NOT NULL
                GROUP BY
                    apiname,
                    deliverySubType
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'rejectionRoleId' col_field,
                    rejectionRoleId col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    rejectionRoleId IS NOT NULL
                GROUP BY
                    apiname,
                    rejectionRoleId
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'isZy' col_field,
                    isZy col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    isZy IS NOT NULL
                GROUP BY
                    apiname,
                    isZy
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'productType' col_field,
                    productType col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    productType IS NOT NULL
                GROUP BY
                    apiname,
                    productType
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'siteDimension' col_field,
                    siteDimension col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    siteDimension IS NOT NULL
                GROUP BY
                    apiname,
                    siteDimension
            )
                t
    )
        t1

UNION ALL

SELECT
    *,
    col_cnt / all_cnt col_rate,
    ROW_NUMBER() over(partition BY apiname order by col_cnt / all_cnt DESC) col_rank
FROM
    (
        SELECT
            apiname,
            col_field,
            col_val,
            col_cnt,
            SUM(col_cnt) over(partition BY apiname) all_cnt
        FROM
            (
                SELECT
                    apiname,
                    'waybillDimension' col_field,
                    waybillDimension col_val,
                    COUNT( *) col_cnt
                FROM
                    mydb.realtime_delivery_invocation
                WHERE
                    waybillDimension IS NOT NULL
                GROUP BY
                    apiname,
                    waybillDimension
            )
                t
    )
        t1;