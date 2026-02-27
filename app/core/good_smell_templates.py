# app/core/good_smell_templates.py

"""
“好味道”代码样本库 (Standard Pattern Library)
该文件定义了 AI 在生成代码时必须参考的优秀范式。
通过 Few-shot Prompting，将这些“教科书”级别的样本注入 AI，
可以确保生成代码的结构、风格和性能都维持在较高水平。
"""

# ==============================================================================
#  DWD (数据仓库明细层) - 好味道范式
# ==============================================================================

DWD_WIDE_TABLE_TEMPLATE = """
-- =================================================
-- DWD 宽表“好味道”范式：使用 CTE 构建清晰的 ETL 流程
-- =================================================
INSERT OVERWRITE TABLE dwm_mall_po_subject_item_df PARTITION(pt='${bizdate}') 
WITH 
-- Step 1: 从 ODS 层加载主表 (例如：订单行表)，并进行基础清洗
source_order_items AS (
    SELECT
        id AS order_item_id,
        order_id,
        product_id,
        quantity,
        price
        -- 添加其他需要的字段...
    FROM ods_mall_order_items
    WHERE pt = '${bizdate}' AND is_deleted = 0 -- 过滤分区和逻辑删除
),

-- Step 2: 关联订单头表，获取订单级信息
joined_orders AS (
    SELECT
        oi.*,
        o.customer_id,
        o.order_time,
        -- 使用 CASE WHEN 进行枚举值转码 (好味道)
        CASE o.status
            WHEN 'PAID' THEN '已支付'
            WHEN 'SHIPPED' THEN '已发货'
            ELSE '未知'
        END AS order_status_desc
    FROM source_order_items AS oi
    LEFT JOIN ods_mall_orders AS o ON oi.order_id = o.id
    WHERE o.pt = '${bizdate}' -- 关联表也需过滤分区
),

-- Step 3: (可选) 关联商品维度表，获取商品信息
final_join AS (
    SELECT
        jo.*,
        p.product_name,
        p.category_name
    FROM joined_orders AS jo
    LEFT JOIN ods_mall_products AS p ON jo.product_id = p.id
)

-- 最终 SELECT，字段对齐、类型转换、注释清晰
SELECT
    order_item_id,
    order_id,
    customer_id,
    product_id,
    product_name,
    category_name,
    order_status_desc,
    CAST(quantity AS BIGINT) AS quantity, -- 显式类型转换 (好味道)
    price
FROM final_join;
"""

# ==============================================================================
#  ADS (数据应用层) - 好味道范式
# ==============================================================================

ADS_AGGREGATION_TEMPLATE = """
-- =================================================
-- ADS 聚合“好味道”范式：清晰的聚合逻辑
-- =================================================

WITH 
-- Step 1: 从 DWD 层加载已经整合好的宽表数据
dwd_source AS (
    SELECT
        customer_id,
        category_name,
        order_time,
        -- DWD 层已经处理好复杂的关联和清洗
        CAST(price * quantity AS DECIMAL(18, 2)) AS item_total_amount
    FROM dwd_mall_sales_detail_df
    WHERE pt = '${bizdate}'
),

-- Step 2: 按需进行聚合计算
aggregated_data AS (
    SELECT
        -- 维度
        customer_id,
        category_name,
        -- 指标
        SUM(item_total_amount) AS total_sales_amount,
        COUNT(DISTINCT order_id) AS total_orders,
        MAX(order_time) AS latest_order_time
    FROM dwd_source
    GROUP BY
        customer_id,
        category_name
)

-- 最终输出，字段名清晰，符合报表需求
SELECT
    customer_id,
    category_name,
    total_sales_amount,
    total_orders,
    latest_order_time
FROM aggregated_data
WHERE total_sales_amount > 1000; -- 可选的聚合后过滤
"""

# ==============================================================================
#  DWS (数据仓库服务层) - 好味道范式
# ==============================================================================

DWS_DETAIL_TEMPLATE = """
-- =================================================
-- DWS 明细报表“好味道”范式：面向分析的直接映射
-- =================================================

-- DWS 层通常是 DWD 层的进一步筛选、重命名或轻度加工，直接服务于 BI
-- 它确保了分析师使用的是一个干净、标准、命名友好的宽表

SELECT
    -- 直接从 DWD 层选择字段
    order_id AS `订单ID`,
    customer_id AS `客户ID`,
    product_name AS `产品名称`,
    category_name AS `品类名称`,
    order_status_desc AS `订单状态`,
    quantity AS `购买数量`,
    price AS `单价`,
    item_total_amount AS `商品总额`,
    order_time AS `下单时间`
FROM dwd_mall_sales_detail_df
WHERE 
    pt = '${bizdate}'
    AND category_name IN ('手机', '家电'); -- 可选的业务过滤
"""

# ==============================================================================
#  DataX ODS - 好味道范式
# ==============================================================================

DATAX_TEMPLATE = """
{
    "job": {
        "setting": {
            "speed": {
                "channel": 5,
                "byte": 10485760
            },
            "errorLimit": {
                "record": 0,
                "percentage": 0.02
            }
        },
        "content": [
            {
                "reader": {
                    "name": "mysqlreader",
                    "parameter": {
                        "username": "${MYSQL_USER}",
                        "password": "${MYSQL_PASSWORD}",
                        "connection": [
                            {
                                "jdbcUrl": [
                                    "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DB}?useUnicode=true&characterEncoding=UTF-8"
                                ],
                                "table": [
                                    "your_source_table"
                                ]
                            }
                        ],
                        "column": [
                            "id",
                            "name",
                            "create_time"
                        ],
                        "where": "1=1"
                    }
                },
                "writer": {
                    "name": "clickhousewriter",
                    "parameter": {
                        "username": "${CLICKHOUSE_USER}",
                        "password": "${CLICKHOUSE_PASSWORD}",
                        "column": [
                            "id",
                            "name",
                            "create_time"
                        ],
                        "connection": [
                            {
                                "jdbcUrl": "jdbc:clickhouse://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT}/${CLICKHOUSE_DB}",
                                "table": [
                                    "your_target_table"
                                ]
                            }
                        ],
                        "preSql": [
                            "TRUNCATE TABLE your_target_table"
                        ],
                        "batchSize": 1024
                    }
                }
            }
        ]
    }
}
"""

# ==============================================================================
#  ClickHouse DDL - 好味道范式
# ==============================================================================

CLICKHOUSE_DDL_TEMPLATE = """
-- =================================================
-- ClickHouse DDL “好味道”范式：分布式表 + 本地表
-- =================================================
-- 1. 创建分布式表 (供上层查询)
-- 分布式表本身不存储数据，作为数据写入和查询的统一入口
DROP TABLE IF EXISTS [database].[table_name] ON CLUSTER '[cluster_name]';
CREATE TABLE IF NOT EXISTS [database].[table_name] ON CLUSTER '[cluster_name]' (
    [columns]
)
ENGINE = Distributed('[cluster_name]', '[database]', '[table_name]_part', rand())
COMMENT '[comment]';

-- 2. 创建本地表 (实际存储数据)
-- ReplicatedMergeTree 引擎保证了数据的高可用和副本一致性
DROP TABLE IF EXISTS [database].[table_name]_part ON CLUSTER '[cluster_name]';
CREATE TABLE IF NOT EXISTS [database].[table_name]_part ON CLUSTER '[cluster_name]' (
    [columns]
)
ENGINE = ReplicatedMergeTree('/clickhouse/[cluster_name]/[database]/tables/{shard}/[table_name]/[bizdate]', '{replica}')}
PARTITION BY [partition_key]
ORDER BY [order_key]
[properties]
COMMENT '[comment]';
"""
