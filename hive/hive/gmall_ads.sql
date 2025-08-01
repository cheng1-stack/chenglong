set hive.exec.mode.local.auto=True;
use gmall;
create database if not exists ads;
--优惠券使用统计
DROP TABLE IF EXISTS ads.ads_coupon_stats;
CREATE EXTERNAL TABLE ads.ads_coupon_stats
(
    `ds`              STRING COMMENT '统计日期',
    `coupon_id`       STRING COMMENT '优惠券ID',
    `coupon_name`     STRING COMMENT '优惠券名称',
    `used_count`      BIGINT COMMENT '使用次数',
    `used_user_count` BIGINT COMMENT '使用人数'
) COMMENT '优惠券使用统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_coupon_stats/';


insert overwrite table ads.ads_coupon_stats
select * from ads.ads_coupon_stats
union
select
    '2025-07-18' ds,
    coupon_id,
    coupon_name,
    cast(sum(used_count_1d) as bigint),
    cast(count(*) as bigint)
from dws.dws_tool_user_coupon_coupon_used_1d
where dt='20250730'
group by coupon_id,coupon_name;

select * from ads.ads_coupon_stats;

DROP TABLE IF EXISTS ads.ads_new_order_user_stats;
CREATE EXTERNAL TABLE ads.ads_new_order_user_stats
(
    `ds`                   STRING COMMENT '统计日期',
    `recent_days`          BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `new_order_user_count` BIGINT COMMENT '新增下单人数'
) COMMENT '新增下单用户统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_new_order_user_stats/';

insert overwrite table ads.ads_new_order_user_stats
select * from ads.ads_new_order_user_stats
union
select
    '2025-07-18' ds,
    recent_days,
    count(*) new_order_user_count
from dws.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
where dt='2025-07-18'
  and order_date_first>=date_add('2025-07-18',-recent_days+1)
group by recent_days;

select * from ads.ads_new_order_user_stats;


DROP TABLE IF EXISTS ads.ads_order_by_province;
CREATE EXTERNAL TABLE ads.ads_order_by_province
(
    `ds`                 STRING COMMENT '统计日期',
    `recent_days`        BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `province_id`        STRING COMMENT '省份ID',
    `province_name`      STRING COMMENT '省份名称',
    `area_code`          STRING COMMENT '地区编码',
    `iso_code`           STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_code_3166_2`    STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `order_count`        BIGINT COMMENT '订单数',
    `order_total_amount` DECIMAL(16, 2) COMMENT '订单金额'
) COMMENT '各省份交易统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_order_by_province/';

insert overwrite table ads.ads_order_by_province
select * from ads.ads_order_by_province
union
select
    '2025-07-18' ds,
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_total_amount_1d
from dws.dws_trade_province_order_1d
where dt='2025-07-18'
union
select
    '2025-07-18' ds,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
        end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
        end order_total_amount
from dws.dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
where dt='2025-07-18';

select *from ads.ads_order_by_province;


DROP TABLE IF EXISTS ads.ads_order_continuously_user_count;
CREATE EXTERNAL TABLE ads.ads_order_continuously_user_count
(
    `ds`                            STRING COMMENT '统计日期',
    `recent_days`                   BIGINT COMMENT '最近天数,7:最近7天',
    `order_continuously_user_count` BIGINT COMMENT '连续3日下单用户数'
) COMMENT '最近7日内连续3日下单用户数统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_order_continuously_user_count/';

insert overwrite table ads.ads_order_continuously_user_count
select * from ads.ads_order_continuously_user_count
union
select
    '2025-07-18',
    7,
    count(distinct(user_id))
from
    (
        select
            user_id,
            datediff(lead(dt,2,'9999-12-31') over(partition by user_id order by dt),dt) diff
        from dws.dws_trade_user_order_1d
        where dt>=date_add('2025-07-18',-6)
    )t1
where diff=2;

select
    count(distinct(user_id))
from
    (
        select
            user_id
        from
            (
                select
                    user_id,
                    date_sub(dt,rank() over(partition by user_id order by dt)) diff
                from dws.dws_trade_user_order_1d
                where dt>=date_add('2025-07-18',-6)
            )t1
        group by user_id,diff
        having count(*)>=3
    )t2;

select
    count(*)
from
    (
        select
            user_id,
            sum(num) s
        from
            (
                select
                    user_id,
                    case dt
                        when '2025-07-18' then 1
                        when '2025-06-29' then 10
                        when '2025-06-28' then 100
                        when '2025-06-27' then 1000
                        when '2025-06-26' then 10000
                        when '2025-07-18' then 100000
                        when '2025-06-24' then 1000000
                        else 0
                        end num
                from dws.dws_trade_user_order_1d
                where dt>=date_add('2025-07-18',-6)
            )t1
        group by user_id
    )t2
where cast(s as string) like "%111%";
select
    count(distinct(user_id))
from
    (
        select
            user_id,
            datediff(lead(dt,2,'9999-12-31') over(partition by user_id order by dt),dt) diff
        from dws.dws_trade_user_order_1d
        where dt>=date_add('2025-07-18',-6)
    )t1
where diff<=3;


select * from ads.ads_order_continuously_user_count;

--各品类商品下单统计
DROP TABLE IF EXISTS ads.ads_order_stats_by_cate;
CREATE EXTERNAL TABLE ads.ads_order_stats_by_cate
(
    `ds`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `category1_id`            STRING COMMENT '一级品类ID',
    `category1_name`          STRING COMMENT '一级品类名称',
    `category2_id`            STRING COMMENT '二级品类ID',
    `category2_name`          STRING COMMENT '二级品类名称',
    `category3_id`            STRING COMMENT '三级品类ID',
    `category3_name`          STRING COMMENT '三级品类名称',
    `order_count`             BIGINT COMMENT '下单数',
    `order_user_count`        BIGINT COMMENT '下单人数'
) COMMENT '各品类商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_order_stats_by_cate/';

insert overwrite table ads.ads_order_stats_by_cate
select * from ads.ads_order_stats_by_cate
union
select
    '2025-07-18' ds,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count
from
    (
        select
            1 recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_count_1d) order_count,
            count(distinct(user_id)) order_user_count
        from dws.dws_trade_user_sku_order_1d
        where dt='2025-07-18'
        group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
        union all
        select
            recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_count),
            count(distinct(if(order_count>0,user_id,null)))
        from
            (
                select
                    recent_days,
                    user_id,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2025-07-18'
            )t1
        group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
    )odr;

select * from ads.ads_order_stats_by_cate;


--各品牌商品下单统计
DROP TABLE IF EXISTS ads.ads_order_stats_by_tm;
CREATE EXTERNAL TABLE ads.ads_order_stats_by_tm
(
    `ds`                      STRING COMMENT '统计日期',
    `recent_days`             BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `tm_id`                   STRING COMMENT '品牌ID',
    `tm_name`                 STRING COMMENT '品牌名称',
    `order_count`             BIGINT COMMENT '下单数',
    `order_user_count`        BIGINT COMMENT '下单人数'
) COMMENT '各品牌商品下单统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_order_stats_by_tm/';

insert overwrite table ads.ads_order_stats_by_tm
select * from ads.ads_order_stats_by_tm
union
select
    '2025-07-18' ds,
    recent_days,
    tm_id,
    tm_name,
    order_count,
    order_user_count
from
    (
        select
            1 recent_days,
            tm_id,
            tm_name,
            sum(order_count_1d) order_count,
            count(distinct(user_id)) order_user_count
        from dws.dws_trade_user_sku_order_1d
        where dt='2025-07-18'
        group by tm_id,tm_name
        union all
        select
            recent_days,
            tm_id,
            tm_name,
            sum(order_count),
            count(distinct(if(order_count>0,user_id,null)))
        from
            (
                select
                    recent_days,
                    user_id,
                    tm_id,
                    tm_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='2025-07-18'
            )t1
        group by recent_days,tm_id,tm_name
    )odr;

select * from ads.ads_order_stats_by_tm;

--下单到支付时间间隔平均值
DROP TABLE IF EXISTS ads.ads_order_to_pay_interval_avg;
CREATE EXTERNAL TABLE ads.ads_order_to_pay_interval_avg
(
    `ds`                        STRING COMMENT '统计日期',
    `order_to_pay_interval_avg` BIGINT COMMENT '下单到支付时间间隔平均值,单位为秒'
) COMMENT '下单到支付时间间隔平均值统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_order_to_pay_interval_avg/';

insert overwrite table ads.ads_order_to_pay_interval_avg
select * from ads.ads_order_to_pay_interval_avg
union
select
    '2025-07-18',
    cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
from dwd.dwd_trade_trade_flow_acc
where dt in ('9999-12-31','2025-07-18')
  and payment_date_id='9999-12-31';

select * from ads.ads_order_to_pay_interval_avg;

DROP TABLE IF EXISTS ads.ads_page_path;
CREATE EXTERNAL TABLE ads.ads_page_path
(
    `ds`          STRING COMMENT '统计日期',
    `source`      STRING COMMENT '跳转起始页面ID',
    `target`      STRING COMMENT '跳转终到页面ID',
    `path_count`  BIGINT COMMENT '跳转次数'
) COMMENT '页面浏览路径分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_page_path/';

insert overwrite table ads.ads_page_path
select * from ads.ads_page_path
union
select
    '20250718' ds,
    source,
    nvl(target,'null'),
    count(*) path_count
from
    (
        select
            concat('step-',rn,':',page_id) source,
            concat('step-',rn+1,':',next_page_id) target
        from
            (
                select
                    page_id,
                    lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
                    row_number() over (partition by session_id order by view_time) rn
                from dwd.dwd_traffic_page_view_inc
                where dt='20250718'
            )t1
    )t2
group by source,target;

select * from ads.ads_page_path;

-- -最近30日各品牌复购率
DROP TABLE IF EXISTS ads.ads_repeat_purchase_by_tm;
CREATE EXTERNAL TABLE ads.ads_repeat_purchase_by_tm
(
    `ds`                  STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近天数,30:最近30天',
    `tm_id`              STRING COMMENT '品牌ID',
    `tm_name`            STRING COMMENT '品牌名称',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT '复购率'
) COMMENT '最近30日各品牌复购率统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_repeat_purchase_by_tm/';

insert overwrite table ads.ads_repeat_purchase_by_tm
select * from ads.ads_repeat_purchase_by_tm
union
select
    '2025-07-18',
    30,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
from
    (
        select
            user_id,
            tm_id,
            tm_name,
            sum(order_count_30d) order_count
        from dws.dws_trade_user_sku_order_nd
        where dt='2025-07-18'
        group by user_id, tm_id,tm_name
    )t1
group by tm_id,tm_name;

select * from ads.ads_repeat_purchase_by_tm;

--各品类商品购物车存量Top3
DROP TABLE IF EXISTS ads.ads_sku_cart_num_top3_by_cate;
CREATE EXTERNAL TABLE ads.ads_sku_cart_num_top3_by_cate
(
    `ds`             STRING COMMENT '统计日期',
    `category1_id`   STRING COMMENT '一级品类ID',
    `category1_name` STRING COMMENT '一级品类名称',
    `category2_id`   STRING COMMENT '二级品类ID',
    `category2_name` STRING COMMENT '二级品类名称',
    `category3_id`   STRING COMMENT '三级品类ID',
    `category3_name` STRING COMMENT '三级品类名称',
    `sku_id`         STRING COMMENT 'SKU_ID',
    `sku_name`       STRING COMMENT 'SKU名称',
    `cart_num`       BIGINT COMMENT '购物车中商品数量',
    `rk`             BIGINT COMMENT '排名'
) COMMENT '各品类商品购物车存量Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_sku_cart_num_top3_by_cate/';

set hive.mapjoin.optimized.hashtable=false;
insert overwrite table ads.ads_sku_cart_num_top3_by_cate
select * from ads.ads_sku_cart_num_top3_by_cate
union
select
    '20250718' ds,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
    (
        select
            sku_id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            cart_num,
            rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
        from
            (
                select
                    sku_id,
                    sum(sku_num) cart_num
                from dwd.dwd_trade_cart_full
                where dt='20250718'
                group by sku_id
            )cart
                left join
            (
                select
                    id,
                    sku_name,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name
                from dim.dim_sku_full
                where ds='20211214'
            )sku
            on cart.sku_id=sku.id
    )t1
where rk<=3;
-- 优化项不应一直禁用，受影响的SQL执行完毕后打开
set hive.mapjoin.optimized.hashtable=true;

select * from ads.ads_sku_cart_num_top3_by_cate;

-- -各品牌商品收藏次数Top3
DROP TABLE IF EXISTS ads.ads_sku_favor_count_top3_by_tm;
CREATE EXTERNAL TABLE ads.ads_sku_favor_count_top3_by_tm
(
    `ds`          STRING COMMENT '统计日期',
    `tm_id`       STRING COMMENT '品牌ID',
    `tm_name`     STRING COMMENT '品牌名称',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `sku_name`    STRING COMMENT 'SKU名称',
    `favor_count` BIGINT COMMENT '被收藏次数',
    `rk`          BIGINT COMMENT '排名'
) COMMENT '各品牌商品收藏次数Top3'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_sku_favor_count_top3_by_tm/';

insert overwrite table ads.ads_sku_favor_count_top3_by_tm
select * from ads.ads_sku_favor_count_top3_by_tm
union
select
    '20250718' ds,
    tm_id,
    tm_name,
    sku_id,
    sku_name,
    favor_add_count_1d,
    rk
from
    (
        select
            tm_id,
            tm_name,
            sku_id,
            sku_name,
            favor_add_count_1d,
            rank() over (partition by tm_id order by favor_add_count_1d desc) rk
        from dws.dws_interaction_sku_favor_add_1d
        where ds='20250718'
    )t1
where rk<=3;

select * from ads.ads_sku_favor_count_top3_by_tm;


--各渠道流量统计
DROP TABLE IF EXISTS ads.ads_traffic_stats_by_channel;
CREATE EXTERNAL TABLE ads.ads_traffic_stats_by_channel
(
    `ds`               STRING COMMENT '统计日期',
    `recent_days`      BIGINT COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
    `channel`          STRING COMMENT '渠道',
    `uv_count`         BIGINT COMMENT '访客人数',
    `avg_duration_sec` BIGINT COMMENT '会话平均停留时长，单位为秒',
    `avg_page_count`   BIGINT COMMENT '会话平均浏览页面数',
    `sv_count`         BIGINT COMMENT '会话数',
    `bounce_rate`      DECIMAL(16, 2) COMMENT '跳出率'
) COMMENT '各渠道流量统计'

    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_traffic_stats_by_channel/';

insert overwrite table ads.ads_traffic_stats_by_channel
select * from ads.ads_traffic_stats_by_channel
union
select
    '2025-07-18' ds,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from dws.dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days

group by recent_days,channel;

select * from ads.ads_traffic_stats_by_channel;

--用户行为漏斗分析
DROP TABLE IF EXISTS ads.ads_user_action;
CREATE EXTERNAL TABLE ads.ads_user_action
(
    `ds`                STRING COMMENT '统计日期',
    `home_count`        BIGINT COMMENT '浏览首页人数',
    `good_detail_count` BIGINT COMMENT '浏览商品详情页人数',
    `cart_count`        BIGINT COMMENT '加购人数',
    `order_count`       BIGINT COMMENT '下单人数',
    `payment_count`     BIGINT COMMENT '支付人数'
) COMMENT '用户行为漏斗分析'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_user_action/';


insert overwrite table ads.ads_user_action
select * from ads.ads_user_action
union
select
    '20250718' ds,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
from
    (
        select
            1 recent_days,
            sum(if(page_id='home',1,0)) home_count,
            sum(if(page_id='good_detail',1,0)) good_detail_count
        from dws.dws_traffic_page_visitor_page_view_1d
        where ds='20250718'
          and page_id in ('home','good_detail')
    )page
        join
    (
        select
            1 recent_days,
            count(*) cart_count
        from dws.dws_trade_user_cart_add_1d
        where dt='2025-07-18'
    )cart
    on page.recent_days=cart.recent_days
        join
    (
        select
            1 recent_days,
            count(*) order_count
        from dws.dws_trade_user_order_1d
        where dt='2025-07-18'
    )ord
    on page.recent_days=ord.recent_days
        join
    (
        select
            1 recent_days,
            count(*) payment_count
        from dws.dws_trade_user_payment_1d
        where dt='2025-07-18'
    )pay
    on page.recent_days=pay.recent_days;

select * from ads.ads_user_action;


--用户变动统计
DROP TABLE IF EXISTS ads.ads_user_change;
CREATE EXTERNAL TABLE ads.ads_user_change
(
    `ds`               STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count`  BIGINT COMMENT '回流用户数'
) COMMENT '用户变动统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_user_change/';

insert overwrite table ads.ads_user_change
select * from ads.ads_user_change
union
select
    churn.ds,
    user_churn_count,
    user_back_count
from
    (
        select
            '20250718' ds,
            count(*) user_churn_count
        from dws.dws_user_user_login_td
        where dt='20250718'
          and login_date_last=date_add('20250718',-7)
    )churn
        join
    (
        select
            '20250718' ds,
            count(*) user_back_count
        from
            (
                select
                    user_id,
                    login_date_last
                from dws.dws_user_user_login_td
                where dt='20250718'
                  and login_date_last = '20250718'
            )t1
                join
            (
                select
                    user_id,
                    login_date_last login_date_previous
                from dws.dws_user_user_login_td
                where dt=date_add('20220608',-1)
            )t2
            on t1.user_id=t2.user_id
        where datediff(login_date_last,login_date_previous)>=8
    )back
    on churn.ds=back.ds;

select * from ads.ads_user_change;



--用户留存率
DROP TABLE IF EXISTS ads.ads_user_retention;
CREATE EXTERNAL TABLE ads.ads_user_retention
(
    `ds`              STRING COMMENT '统计日期',
    `create_date`     STRING COMMENT '用户新增日期',
    `retention_day`   INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count`  BIGINT COMMENT '新增用户数量',
    `retention_rate`  DECIMAL(16, 2) COMMENT '留存率'
) COMMENT '用户留存率'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_user_retention/';

insert overwrite table ads.ads_user_retention
select * from ads.ads_user_retention
union
select '20250718' ds,
       login_date_first create_date,
       datediff('20250718', login_date_first) retention_day,
       sum(if(login_date_last = '20250718', 1, 0)) retention_count,
       count(*) new_user_count,
       cast(sum(if(login_date_last = '20250718', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
from (
         select user_id,
                login_date_last,
                login_date_first
         from dws.dws_user_user_login_td
         where dt = '20250718'

     ) t1
group by login_date_first;

select * from ads.ads_user_retention;


-- -用户新增活跃统计
DROP TABLE IF EXISTS ads.ads_user_stats;
CREATE EXTERNAL TABLE ads.ads_user_stats
(
    `ds`                STRING COMMENT '统计日期',
    `recent_days`       BIGINT COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
    `new_user_count`    BIGINT COMMENT '新增用户数',
    `active_user_count` BIGINT COMMENT '活跃用户数'
) COMMENT '用户新增活跃统计'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
    LOCATION '/bigdata_warehouse/gmall/ads/ads_user_stats/';

insert overwrite table ads.ads_user_stats
select * from ads.ads_user_stats
union
select '20250718' ds,
       recent_days,
       sum(if(login_date_first >= date_add('20250718', -recent_days + 1), 1, 0)) new_user_count,
       count(*) active_user_count
from dws.dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
group by recent_days;

select * from  ads.ads_user_stats;


























