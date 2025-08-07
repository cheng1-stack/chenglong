create database if not exists newbiaodan;
use newbiaodan;
-- ODS 层：商品基础信息表
CREATE TABLE if not exists ods_product_base (
product_id STRING COMMENT '商品ID',
product_name STRING COMMENT '商品名称',
category_leaf STRING COMMENT '叶子类目',
price DECIMAL(10,2) COMMENT '价格',
create_time STRING COMMENT '创建时间'
)
COMMENT 'ODS层-商品基础信息表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ods_product_base/'
TBLPROPERTIES ('orc.compress' = 'SNAPPY');

-- ODS 层：商品访问行为表
CREATE TABLE if not exists ods_product_visit (
product_id STRING COMMENT '商品ID',
stat_period STRING COMMENT '统计日期',
visitor_count INT COMMENT '访客数',
view_count INT COMMENT '浏览量',
avg_stay_time INT COMMENT '平均停留时长(秒)',
bounce_rate DECIMAL(3,2) COMMENT '跳出率'
)
COMMENT 'ODS层-商品访问行为表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ods_product_visit/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 层：商品收藏加购表
CREATE TABLE if not exists ods_product_collect_cart (
product_id STRING COMMENT '商品ID',
stat_period STRING COMMENT '统计日期',
collect_user_count INT COMMENT '收藏人数',
add_cart_user_count INT COMMENT '加购人数',
add_cart_item_count INT COMMENT '加购件数'
)
COMMENT 'ODS层-商品收藏加购表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ods_product_collect_cart/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 层：商品下单支付表
CREATE TABLE if not exists ods_product_pay (
product_id STRING COMMENT '商品ID',
stat_period STRING COMMENT '统计日期',
order_user_count INT COMMENT '下单买家数',
pay_user_count INT COMMENT '支付买家数',
pay_amount DECIMAL(10,2) COMMENT '支付金额',
refund_amount DECIMAL(10,2) COMMENT '退款金额'
)
COMMENT 'ODS层-商品下单支付表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ods_product_collect_cart/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 层：SKU销售表
CREATE TABLE if not exists ods_sku_sale (
sku_id STRING COMMENT 'SKU ID',
product_id STRING COMMENT '商品ID',
stat_period STRING COMMENT '统计日期',
pay_item_count INT COMMENT '支付件数',
current_stock INT COMMENT '当前库存',
stock_days INT COMMENT '可售天数'
)
COMMENT 'ODS层-SKU销售表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ods_sku_sale/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 层：流量来源表
CREATE TABLE if not exists ods_traffic_source (
product_id STRING COMMENT '商品ID',
stat_period STRING COMMENT '统计日期',
source_type STRING COMMENT '来源类型',
visitor_count INT COMMENT '来源访客数',
pay_conv_rate DECIMAL(3,2) COMMENT '来源支付转化率'
)
COMMENT 'ODS层-流量来源表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ods_traffic_source/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ODS 层：搜索词表
CREATE TABLE if not exists ods_search_words (
product_id STRING COMMENT '商品ID',
stat_period STRING COMMENT '统计日期',
search_word STRING COMMENT '搜索词',
search_count INT COMMENT '搜索次数'
)
COMMENT 'ODS层-搜索词表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ods_search_words/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- DWD 层：商品访问明细
CREATE TABLE if not exists dwd_product_visit_detail (
product_id STRING COMMENT '商品ID',
product_name STRING COMMENT '商品名称',
category_leaf STRING COMMENT '叶子类目',
stat_period STRING COMMENT '统计日期',
visitor_count INT COMMENT '访客数',
view_count INT COMMENT '浏览量',
bounce_rate DECIMAL(3,2) COMMENT '跳出率'
)
COMMENT 'DWD层-商品访问明细'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/dwd_product_visit_detail/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- DWD 层：商品支付明细
CREATE TABLE if not exists dwd_product_pay_detail (
product_id STRING COMMENT '商品ID',
category_leaf STRING COMMENT '叶子类目',
stat_period STRING COMMENT '统计日期',
order_user_count INT COMMENT '下单买家数',
pay_user_count INT COMMENT '支付买家数',
pay_amount DECIMAL(10,2) COMMENT '支付金额',
pay_conv_rate DECIMAL(3,2) COMMENT '支付转化率'
)
COMMENT 'DWD层-商品支付明细'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/dwd_product_pay_detail/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- DWD 层：SKU销售明细
CREATE TABLE if not exists dwd_sku_sale_detail (
sku_id STRING COMMENT 'SKU ID',
product_id STRING COMMENT '商品ID',
product_name STRING COMMENT '商品名称',
stat_period STRING COMMENT '统计日期',
pay_item_count INT COMMENT '支付件数',
current_stock INT COMMENT '当前库存'
)
COMMENT 'DWD层-SKU销售明细'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/dwd_sku_sale_detail/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- DWS 层：商品销售日汇总
CREATE TABLE if not exists dws_product_sale_d (
product_id STRING COMMENT '商品ID',
product_name STRING COMMENT '商品名称',
category_leaf STRING COMMENT '叶子类目',
stat_period STRING COMMENT '统计日期',
total_pay_amount DECIMAL(10,2) COMMENT '总支付金额',
total_pay_item INT COMMENT '总支付件数',
total_visitor INT COMMENT '总访客数',
pay_conv_rate DECIMAL(3,2) COMMENT '日均支付转化率'
)
COMMENT 'DWS层-商品销售日汇总'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/dws_product_sale_d/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- DWS 层：类目销售日汇总
CREATE TABLE if not exists dws_product_category_sale_d (
category_leaf STRING COMMENT '叶子类目',
stat_period STRING COMMENT '统计日期',
total_pay_amount DECIMAL(10,2) COMMENT '总支付金额',
total_product_num INT COMMENT '类目商品数',
avg_pay_amount DECIMAL(10,2) COMMENT '类目客单价'
)
COMMENT 'DWS层-类目销售日汇总'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/dws_product_category_sale_d/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- DWS 层：商品流量日汇总
CREATE TABLE if not exists dws_product_traffic_d (
product_id STRING COMMENT '商品ID',
stat_period STRING COMMENT '统计日期',
top_source_type STRING COMMENT 'TOP1流量来源',
top_source_visitor INT COMMENT 'TOP1来源访客数',
total_visitor INT COMMENT '总访客数'
)
COMMENT 'DWS层-商品流量日汇总'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/dws_product_traffic_d/'
TBLPROPERTIES ('orc.compress'='SNAPPY');


-- ADS 层：商品排行表
CREATE TABLE if not exists   ads_product_rank (
product_id STRING COMMENT '商品ID',
product_name STRING COMMENT '商品名称',
category_leaf STRING COMMENT '叶子类目',
rank_period STRING COMMENT '排名周期',
sales_rank INT COMMENT '销售额排名',
volume_rank INT COMMENT '销量排名',
total_pay_amount DECIMAL(10,2) COMMENT '总支付金额',
total_pay_item INT COMMENT '总支付件数'
)
COMMENT 'ADS层-商品排行表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ads_product_rank/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ADS 层：TOP5 SKU销售表
CREATE TABLE if not exists   ads_sku_top5 (
product_id STRING COMMENT '商品ID',
rank_period STRING COMMENT '排名周期',
sku_id STRING COMMENT 'SKU ID',
pay_item_count INT COMMENT '支付件数',
current_stock INT COMMENT '当前库存',
stock_days INT COMMENT '可售天数'
)
COMMENT 'ADS层-TOP5 SKU销售表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ads_sku_top5/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ADS 层：TOP10流量来源表
CREATE TABLE if not exists   ads_traffic_top10 (
product_id STRING COMMENT '商品ID',
rank_period STRING COMMENT '排名周期',
source_type STRING COMMENT '来源类型',
visitor_count INT COMMENT '访客数',
pay_conv_rate DECIMAL(3,2) COMMENT '支付转化率'
)
COMMENT 'ADS层-TOP10流量来源表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ads_traffic_top10/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ADS 层：TOP10搜索词表
CREATE TABLE if not exists   ads_search_word_top10 (
product_id STRING COMMENT '商品ID',
rank_period STRING COMMENT '排名周期',
search_word STRING COMMENT '搜索词',
search_count INT COMMENT '搜索次数'
)
COMMENT 'ADS层-TOP10搜索词表'
PARTITIONED BY (dt STRING )
STORED AS ORC
LOCATION '/warehouse/new/ads_traffic_top10/'
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- 设置动态分区模式
set hive.exec.dynamic.partition.mode=nonstrict;
-- 定义日期变量（实际执行时替换为具体日期，如 '2025-08-07'）
set dt=${dt};

-- 1. 向 ods_product_base 插入 10000 条数据
INSERT INTO TABLE ods_product_base PARTITION (dt)
SELECT
CONCAT('P', LPAD(CAST(pos + 1 AS STRING), 5, '0')) AS product_id,  -- 商品ID：P00001~P10000
CONCAT('商品', LPAD(CAST(pos + 1 AS STRING), 5, '0')) AS product_name,  -- 商品名称
CASE CAST(RAND() * 3 AS INT)  -- 随机叶子类目
WHEN 0 THEN '服饰-男装-T恤'
WHEN 1 THEN '电子-手机-智能手机'
ELSE '家居-厨具-炒锅'
END AS category_leaf,
ROUND(RAND() * 989 + 10, 2) AS price,  -- 价格：10~999元
DATE_FORMAT(DATE_ADD(FROM_UNIXTIME(UNIX_TIMESTAMP()), -CAST(RAND() * 365 AS INT)), 'yyyy-MM-dd') AS create_time,  -- 随机创建时间（近1年）
${dt} AS dt  -- 分区变量
FROM (
SELECT pos FROM (SELECT posexplode(split(space(9999), ' ')) AS (pos, val)) t  -- 生成 0~9999 共10000条
) t;


-- 2. 向 ods_product_visit 插入 10000 条数据
INSERT INTO TABLE ods_product_visit PARTITION (dt)
SELECT
CONCAT('P', LPAD(CAST(pos + 1 AS STRING), 5, '0')) AS product_id,  -- 关联商品ID
${dt} AS stat_period,  -- 统计日期=分区日期
CAST(RAND() * 450 + 50 AS INT) AS visitor_count,  -- 访客数：50~499
CAST(visitor_count * (RAND() * 2 + 1) AS INT) AS view_count,  -- 浏览量：访客数1~3倍
CAST(RAND() * 170 + 10 AS INT) AS avg_stay_time,  -- 平均停留时长：10~179秒
ROUND(RAND() * 0.6 + 0.1, 2) AS bounce_rate,  -- 跳出率：0.1~0.7
${dt} AS dt
FROM (
SELECT pos, CAST(RAND() * 450 + 50 AS INT) AS visitor_count
FROM (SELECT posexplode(split(space(9999), ' ')) AS (pos, val)) t
) t;



-- 3. 向 ods_product_collect_cart 插入 10000 条数据
INSERT INTO TABLE ods_product_collect_cart PARTITION (dt)
SELECT
CONCAT('P', LPAD(CAST(pos + 1 AS STRING), 5, '0')) AS product_id,
${dt} AS stat_period,
CAST(visitor_count * (RAND() * 0.15 + 0.05) AS INT) AS collect_user_count,  -- 收藏人数：访客数5%~20%
CAST(visitor_count * (RAND() * 0.2 + 0.1) AS INT) AS add_cart_user_count,  -- 加购人数：访客数10%~30%
CAST((CAST(visitor_count * (RAND() * 0.2 + 0.1) AS INT) ) * (RAND() + 1) AS INT) AS add_cart_item_count,  -- 加购件数：加购人数1~2倍
${dt} AS dt
FROM (
SELECT pos, CAST(RAND() * 450 + 50 AS INT) AS visitor_count  -- 复用访客数逻辑
FROM (SELECT posexplode(split(space(9999), ' ')) AS (pos, val)) t
) t;



-- 4. 向 ods_product_pay 插入 10000 条数据
INSERT INTO TABLE ods_product_pay PARTITION (dt)
SELECT
product_id,
${dt} AS stat_period,
order_user_count,
CAST(order_user_count * (RAND() * 0.3 + 0.6) AS INT) AS pay_user_count,
pay_amount,
ROUND(pay_amount * RAND() * 0.05, 2) AS refund_amount,
${dt} AS dt
FROM (
SELECT
product_id,
order_user_count,
pay_item_count,
price,
pos,
ROUND(pay_item_count * price * (RAND() * 0.4 + 0.8), 2) AS pay_amount
FROM (
SELECT
CONCAT('P', LPAD(CAST(pos + 1 AS STRING), 5, '0')) AS product_id,
CAST(add_cart_user_count * (RAND() * 0.3 + 0.2) AS INT) AS order_user_count,
pay_item_count,
price,
pos
FROM (
SELECT
pos,
CAST(RAND() * 450 + 50 AS INT) AS add_cart_user_count,
CAST(RAND() * 3 + 1 AS INT) AS pay_item_count,
ROUND(RAND() * 989 + 10, 2) AS price
FROM (
SELECT posexplode(split(space(9999), ' ')) AS (pos, val)
) t1
) t2
) t3
) t4;




-- 5. 向 ods_sku_sale 插入 10000 条数据
INSERT INTO TABLE ods_sku_sale PARTITION (dt)
SELECT
CONCAT('SKU_', product_id, '_', CAST(RAND() * 3 + 1 AS INT)) AS sku_id,  -- 每个商品1~3个SKU
product_id,
${dt} AS stat_period,
CAST(RAND() * 100 + 1 AS INT) AS pay_item_count,  -- 支付件数1~100
CAST(RAND() * 1000 + 100 AS INT) AS current_stock,  -- 当前库存100~1100
CAST(current_stock / ((CAST(RAND() * 100 + 1 AS INT) ) + 1) AS INT) AS stock_days,  -- 可售天数=库存/支付件数
${dt} AS dt
FROM (
SELECT CONCAT('P', LPAD(CAST(pos + 1 AS STRING), 5, '0')) AS product_id,
CAST(RAND() * 1000 + 100 AS INT) AS current_stock
FROM (SELECT posexplode(split(space(9999), ' ')) AS (pos, val)) t
) t;



-- 6. 向 ods_traffic_source 插入 10000 条数据
INSERT INTO TABLE ods_traffic_source PARTITION (dt)
SELECT
CONCAT('P', LPAD(CAST(pos + 1 AS STRING), 5, '0')) AS product_id,
${dt} AS stat_period,
source_type,  -- 随机流量来源
CAST(RAND() * 500 + 10 AS INT) AS visitor_count,  -- 来源访客数10~510
ROUND(RAND() * 0.5 + 0.1, 2) AS pay_conv_rate,  -- 支付转化率0.1~0.6
${dt} AS dt
FROM (
SELECT
pos,
CASE CAST(RAND() * 10 AS INT)  -- 流量来源列表
WHEN 0 THEN '效果广告'
WHEN 1 THEN '手淘搜索'
WHEN 2 THEN '内容广告'
WHEN 3 THEN '站外广告'
WHEN 4 THEN '购物车'
WHEN 5 THEN '我的淘宝'
WHEN 6 THEN '手淘推荐'
WHEN 7 THEN '品牌广告'
WHEN 8 THEN '手淘其他店铺'
ELSE '淘内待分类'
END AS source_type
FROM (SELECT posexplode(split(space(9999), ' ')) AS (pos, val)) t
) t;



-- 7. 向 ods_search_words 插入 10000 条数据
INSERT INTO TABLE ods_search_words PARTITION (dt)
SELECT
CONCAT('P', LPAD(CAST(pos + 1 AS STRING), 5, '0')) AS product_id,
${dt} AS stat_period,
search_word,  -- 随机搜索词
CAST(RAND() * 1000 + 10 AS INT) AS search_count,  -- 搜索次数10~1010
${dt} AS dt
FROM (
SELECT
pos,
CASE CAST(RAND() * 5 AS INT)  -- 预设搜索词
WHEN 0 THEN '轩妈家'
WHEN 1 THEN '男装T恤'
WHEN 2 THEN '智能手机'
WHEN 3 THEN '炒锅 家用'
ELSE CONCAT('商品', CAST(RAND() * 1000 AS INT))
END AS search_word
FROM (SELECT posexplode(split(space(9999), ' ')) AS (pos, val)) t
) t;
