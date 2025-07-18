-- 1. dwd_trade_order_detail_inc
-- 1.1 首日装载
use tms02;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trade_order_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
       dt,
       date_format(order_time, 'yyyy-MM-dd') dt1
from (select id,
             order_id,
             cargo_type,
           volume_length,
             volume_width,
             volume_height,
             weight,
             concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) order_time,
             dt
      from  ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);

-- 1.2 每日装载
insert overwrite table tms02.dwd_trade_order_detail_inc
    partition (dt = '2025-07-13')
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name   cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name       status_name,
       collect_type,
       dic_for_collect_type.name collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             date_format(
                     from_utc_timestamp(
                                 to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                          substr(create_time, 12, 8))) * 1000,
                                 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
             dt
      from ods_order_cargo
      where dt = '2025-07-13'
        ) cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance
      from ods_order_info
      where dt = '2025-07-13'
        ) info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);

-- 2. dwd_trade_pay_suc_detail_inc
-- 2.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trade_pay_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       payment_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       date_format(payment_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
           dt
      from ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) payment_time
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 2.2 每日装载
with pay_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 payment_time,
               dt
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') payment_time,
                     dt
                from ods_order_info
                where dt = '2025-07-13'
                  
                  and status = '60010'
                  and status = '60020'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and status = '60010'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                    dt
               from ods_order_cargo
               where dt = '2025-07-13'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where dt = '2025-07-13'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms02.dwd_trade_pay_suc_detail_inc
    partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       pay_info.payment_time,
       order_info.order_no,
       pay_info.status,
       pay_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       pay_info.payment_type,
       pay_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       pay_info.dt
from pay_info
         join order_info
              on pay_info.id = order_info.order_id;

-- 3. dwd_trade_order_cancel_detail_inc
-- 3.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trade_order_cancel_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       cancel_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       date_format(cancel_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
           dt
      from ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) cancel_time
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0'
        and status = '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string);

-- 3.2 每日装载
with cancel_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 cancel_time,
               dt
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') cancel_time,
                     dt
                from ods_order_info
                where dt = '2025-07-13'
                  
                  and status = '60999'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                    dt
               from ods_order_cargo
               where dt = '2025-07-13'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where dt = '2025-07-13'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms02.dwd_trade_order_cancel_detail_inc
    partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       cancel_info.cancel_time,
       order_info.order_no,
       cancel_info.status,
       cancel_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       cancel_info.dt
from cancel_info
         join order_info
              on cancel_info.id = order_info.order_id;

-- 4. dwd_trans_receive_detail_inc
-- 4.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trans_receive_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       receive_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       date_format(receive_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
           dt
      from ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) receive_time
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 4.2 每日装载
with receive_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 receive_time,
               dt
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') receive_time,
                     dt
                from ods_order_info
                where dt = '2025-07-13'
                  
                  and status = '60020'
                  and status = '60030'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                    dt
               from ods_order_cargo
               where dt = '2025-07-13'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where dt = '2025-07-13'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms02.dwd_trans_receive_detail_inc
    partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       receive_info.receive_time,
       order_info.order_no,
       receive_info.status,
       receive_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       receive_info.payment_type,
       receive_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       receive_info.dt
from receive_info
         join order_info
              on receive_info.id = order_info.order_id;

-- 5. dwd_trans_dispatch_detail_inc
-- 5.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trans_dispatch_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       dispatch_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       date_format(dispatch_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
           dt
      from ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) dispatch_time
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 5.2 每日装载
with dispatch_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 dispatch_time,
               dt
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') dispatch_time,
                     dt
                from ods_order_info
                where dt = '2025-07-13'
                  
                  and status = '60040'
                  and status = '60050'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                    dt
               from ods_order_cargo
               where dt = '2025-07-13'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where dt = '2025-07-13'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms02.dwd_trans_dispatch_detail_inc
    partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       dispatch_info.dispatch_time,
       order_info.order_no,
       dispatch_info.status,
       dispatch_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       dispatch_info.payment_type,
       dispatch_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       dispatch_info.dt
from dispatch_info
         join order_info
              on dispatch_info.id = order_info.order_id;

-- 6. dwd_trans_bound_finish_detail_inc
-- 6.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trans_bound_finish_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       bound_finish_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       date_format(bound_finish_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
           dt
      from ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) bound_finish_time
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60050'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 6.2 每日装载
with bound_finish_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 bound_finish_time,
               dt
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') bound_finish_time,
                     dt
                from ods_order_info
                where dt = '2025-07-13'
                  
                  and status = '60050'
                  and status = '60060'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040' or
                status = '60050')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                    dt
               from ods_order_cargo
               where dt = '2025-07-13'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where dt = '2025-07-13'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms02.dwd_trans_bound_finish_detail_inc
    partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       bound_finish_info.bound_finish_time,
       order_info.order_no,
       bound_finish_info.status,
       bound_finish_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       bound_finish_info.payment_type,
       bound_finish_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       bound_finish_info.dt
from bound_finish_info
         join order_info
              on bound_finish_info.id = order_info.order_id;

-- 7. dwd_trans_deliver_suc_detail_inc
-- 7.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trans_deliver_suc_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       deliver_suc_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       date_format(deliver_suc_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
           dt
      from ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) deliver_suc_time
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60050'
        and status <> '60060'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 7.2 每日装载
with deliver_suc_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 deliver_suc_time,
               dt
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') deliver_suc_time,
                     dt
                from ods_order_info
                where dt = '2025-07-13'
                  
                  and status = '60060'
                  and status = '60070'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
           and (status = '60010' or
                status = '60020' or
                status = '60030' or
                status = '60040' or
                status = '60050' or
                status = '60060')
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                    dt
               from ods_order_cargo
               where dt = '2025-07-13'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where dt = '2025-07-13'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms02.dwd_trans_deliver_suc_detail_inc
    partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       deliver_suc_info.deliver_suc_time,
       order_info.order_no,
       deliver_suc_info.status,
       deliver_suc_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       deliver_suc_info.payment_type,
       deliver_suc_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       deliver_suc_info.dt
from deliver_suc_info
         join order_info
              on deliver_suc_info.id = order_info.order_id;

-- 8. dwd_trans_sign_detail_inc
-- 8.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trans_sign_detail_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name                 cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       sign_time,
       order_no,
       status,
       dic_for_status.name                     status_name,
       collect_type,
       dic_for_collect_type.name               collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name               payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       date_format(sign_time, 'yyyy-MM-dd') dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
           dt
      from ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*')                                  receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')                                    sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')                                              estimate_arrive_time,
             distance,
             concat(substr(update_time, 1, 10), ' ', substr(update_time, 12, 8)) sign_time
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0'
        and status <> '60010'
        and status <> '60020'
        and status <> '60030'
        and status <> '60040'
        and status <> '60050'
        and status <> '60060'
        and status <> '60070'
        and status <> '60999') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 8.2 每日装载
with sign_info
         as
         (select without_status.id,
                 order_no,
                 status,
                 dic_for_status.name status_name,
                 collect_type,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_type_name.name  payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
                 sign_time,
               dt
          from (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*')       receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')         sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')                   estimate_arrive_time,
                       distance,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(update_time, 1, 10), ' ',
                                                                    substr(update_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') sign_time,
                     dt
                from ods_order_info
                where dt = '2025-07-13'
                  
                  and status = '60070'
                  and status = '60080'
                  and is_deleted = '0') without_status
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_status
               on without_status.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_type_name
               on without_status.payment_type = cast(dic_type_name.id as string)),
     order_info
         as (
         select id,
                order_id,
                cargo_type,
                cargo_type_name,
                volumn_length,
                volumn_width,
                volumn_height,
                weight,
                order_time,
                order_no,
                status,
                status_name,
                collect_type,
                collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                payment_type,
                payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from dwd_trade_order_process_inc
         where dt = '9999-12-31'
         union
         select cargo.id,
                order_id,
                cargo_type,
                dic_for_cargo_type.name   cargo_type_name,
                volume_length,
                volume_width,
                volume_height,
                weight,
                order_time,
                order_no,
                status,
                dic_for_status.name       status_name,
                collect_type,
                dic_for_collect_type.name collect_type_name,
                user_id,
                receiver_complex_id,
                receiver_province_id,
                receiver_city_id,
                receiver_district_id,
                receiver_name,
                sender_complex_id,
                sender_province_id,
                sender_city_id,
                sender_district_id,
                sender_name,
                ''                        payment_type,
                ''                        payment_type_name,
                cargo_num,
                amount,
                estimate_arrive_time,
                distance
         from (select id,
                      order_id,
                      cargo_type,
                      volume_length,
                      volume_width,
                      volume_height,
                      weight,
                      date_format(
                              from_utc_timestamp(
                                          to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                   substr(create_time, 12, 8))) * 1000,
                                          'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                    dt
               from ods_order_cargo
               where dt = '2025-07-13'
                 ) cargo
                  join
              (select id,
                      order_no,
                      status,
                      collect_type,
                      user_id,
                      receiver_complex_id,
                      receiver_province_id,
                      receiver_city_id,
                      receiver_district_id,
                      concat(substr(receiver_name, 1, 1), '*') receiver_name,
                      sender_complex_id,
                      sender_province_id,
                      sender_city_id,
                      sender_district_id,
                      concat(substr(sender_name, 1, 1), '*')   sender_name,
                      cargo_num,
                      amount,
                      date_format(from_utc_timestamp(
                                          cast(estimate_arrive_time as bigint), 'UTC'),
                                  'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                      distance
               from ods_order_info
               where dt = '2025-07-13'
                 ) info
              on cargo.order_id = info.id
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_cargo_type
              on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_status
              on info.status = cast(dic_for_status.id as string)
                  left join
              (select id,
                      name
               from ods_base_dic
               where dt = '2025-07-13'
                 and is_deleted = '0') dic_for_collect_type
              on info.collect_type = cast(dic_for_cargo_type.id as string))
insert overwrite table tms02.dwd_trans_sign_detail_inc
    partition(dt = '2025-07-13')
select order_info.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       sign_info.sign_time,
       order_info.order_no,
       sign_info.status,
       sign_info.status_name,
       order_info.collect_type,
       collect_type_name,
       order_info.user_id,
       order_info.receiver_complex_id,
       order_info.receiver_province_id,
       order_info.receiver_city_id,
       order_info.receiver_district_id,
       order_info.receiver_name,
       order_info.sender_complex_id,
       order_info.sender_province_id,
       order_info.sender_city_id,
       order_info.sender_district_id,
       order_info.sender_name,
       sign_info.payment_type,
       sign_info.payment_type_name,
       order_info.cargo_num,
       order_info.amount,
       order_info.estimate_arrive_time,
       order_info.distance,
       sign_info.dt
from sign_info
         join order_info
              on sign_info.id = order_info.order_id;

-- 9. dwd_trade_order_process_inc
-- 9.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table tms02.dwd_trade_order_process_inc
    partition (dt)
select cargo.id,
       order_id,
       cargo_type,
       dic_for_cargo_type.name               cargo_type_name,
       volume_length,
       volume_width,
       volume_height,
       weight,
       order_time,
       order_no,
       status,
       dic_for_status.name                   status_name,
       collect_type,
       dic_for_collect_type.name             collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       payment_type,
       dic_for_payment_type.name             payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       date_format(order_time, 'yyyy-MM-dd') start_date,
       end_date,
       end_date                              dt
from (select id,
             order_id,
             cargo_type,
             volume_length,
             volume_width,
             volume_height,
             weight,
             concat(substr(create_time, 1, 10), ' ', substr(create_time, 12, 8)) order_time,
           dt
      from ods_order_cargo
      where dt = '2025-07-13'
        and is_deleted = '0') cargo
         join
     (select id,
             order_no,
             status,
             collect_type,
             user_id,
             receiver_complex_id,
             receiver_province_id,
             receiver_city_id,
             receiver_district_id,
             concat(substr(receiver_name, 1, 1), '*') receiver_name,
             sender_complex_id,
             sender_province_id,
             sender_city_id,
             sender_district_id,
             concat(substr(sender_name, 1, 1), '*')   sender_name,
             payment_type,
             cargo_num,
             amount,
             date_format(from_utc_timestamp(
                                 cast(estimate_arrive_time as bigint), 'UTC'),
                         'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
             distance,
             if(status = '60080' or
                status = '60999',
                concat(substr(update_time, 1, 10)),
                '9999-12-31')                               end_date
      from ods_order_info
      where dt = '2025-07-13'
        and is_deleted = '0') info
     on cargo.order_id = info.id
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_cargo_type
     on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_status
     on info.status = cast(dic_for_status.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_collect_type
     on info.collect_type = cast(dic_for_cargo_type.id as string)
         left join
     (select id,
             name
      from ods_base_dic
      where dt = '2025-07-13'
        and is_deleted = '0') dic_for_payment_type
     on info.payment_type = cast(dic_for_payment_type.id as string);

-- 9.2 每日装载
with tmp
         as
         (select id,
                 order_id,
                 cargo_type,
                 cargo_type_name,
                 volumn_length,
                 volumn_width,
                 volumn_height,
                 weight,
                 order_time,
                 order_no,
                 status,
                 status_name,
                 collect_type,
                 collect_type_name,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
               dt,
                 start_date,
                 end_date
          from dwd_trade_order_process_inc
          where dt = '9999-12-31'
          union
          select cargo.id,
                 order_id,
                 cargo_type,
                 dic_for_cargo_type.name               cargo_type_name,
                 volume_length,
                 volume_width,
                 volume_height,
                 weight,
                 order_time,
                 order_no,
                 status,
                 dic_for_status.name                   status_name,
                 collect_type,
                 dic_for_collect_type.name             collect_type_name,
                 user_id,
                 receiver_complex_id,
                 receiver_province_id,
                 receiver_city_id,
                 receiver_district_id,
                 receiver_name,
                 sender_complex_id,
                 sender_province_id,
                 sender_city_id,
                 sender_district_id,
                 sender_name,
                 payment_type,
                 dic_for_payment_type.name             payment_type_name,
                 cargo_num,
                 amount,
                 estimate_arrive_time,
                 distance,
               dt,
                 date_format(order_time, 'yyyy-MM-dd') start_date,
                 '9999-12-31'                          end_date
          from (select id,
                       order_id,
                       cargo_type,
                       volume_length,
                       volume_width,
                       volume_height,
                       weight,
                       date_format(
                               from_utc_timestamp(
                                           to_unix_timestamp(concat(substr(create_time, 1, 10), ' ',
                                                                    substr(create_time, 12, 8))) * 1000,
                                           'GMT+8'), 'yyyy-MM-dd HH:mm:ss') order_time,
                     dt
                from ods_order_cargo
                where dt = '2025-07-13'
                  ) cargo
                   join
               (select id,
                       order_no,
                       status,
                       collect_type,
                       user_id,
                       receiver_complex_id,
                       receiver_province_id,
                       receiver_city_id,
                       receiver_district_id,
                       concat(substr(receiver_name, 1, 1), '*') receiver_name,
                       sender_complex_id,
                       sender_province_id,
                       sender_city_id,
                       sender_district_id,
                       concat(substr(sender_name, 1, 1), '*')   sender_name,
                       payment_type,
                       cargo_num,
                       amount,
                       date_format(from_utc_timestamp(
                                           cast(estimate_arrive_time as bigint), 'UTC'),
                                   'yyyy-MM-dd HH:mm:ss')             estimate_arrive_time,
                       distance
                from ods_order_info
                where dt = '2025-07-13'
                  ) info
               on cargo.order_id = info.id
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_cargo_type
               on cargo.cargo_type = cast(dic_for_cargo_type.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_status
               on info.status = cast(dic_for_status.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_collect_type
               on info.collect_type = cast(dic_for_cargo_type.id as string)
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_payment_type
               on info.payment_type = cast(dic_for_payment_type.id as string)),
     inc
         as
         (select without_type_name.id,
                 status,
                 payment_type,
                 dic_for_payment_type.name payment_type_name
          from (select id,
                       status,
                       payment_type
                from (select id,
                             status,
                             payment_type,
                             row_number() over (partition by id order by dt desc) rn
                      from ods_order_info
                      where dt = '2025-07-13'
                        
                        and is_deleted = '0'
                     ) inc_origin
                where rn = 1) without_type_name
                   left join
               (select id,
                       name
                from ods_base_dic
                where dt = '2025-07-13'
                  and is_deleted = '0') dic_for_payment_type
               on without_type_name.payment_type = cast(dic_for_payment_type.id as string)
         )
insert overwrite table dwd_trade_order_process_inc
    partition(dt)
select tmp.id,
       order_id,
       cargo_type,
       cargo_type_name,
       volumn_length,
       volumn_width,
       volumn_height,
       weight,
       order_time,
       order_no,
       inc.status,
       status_name,
       collect_type,
       collect_type_name,
       user_id,
       receiver_complex_id,
       receiver_province_id,
       receiver_city_id,
       receiver_district_id,
       receiver_name,
       sender_complex_id,
       sender_province_id,
       sender_city_id,
       sender_district_id,
       sender_name,
       inc.payment_type,
       inc.payment_type_name,
       cargo_num,
       amount,
       estimate_arrive_time,
       distance,
     dt,
       start_date,
       if(inc.status = '60080' or
          inc.status = '60999',
          '2025-07-13', tmp.end_date) end_date,
       if(inc.status = '60080' or
          inc.status = '60999',
          '2025-07-13', tmp.end_date) dt
from tmp
         left join inc
                   on tmp.order_id = inc.id;

-- 10. dwd_trans_trans_finish_inc
-- 10.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trans_trans_finish_inc
    partition (dt)
select id2  ,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       estimated_time estimate_end_time,
       actual_distance,
       finish_dur_sec,
     dt,
       dt1
from (select id id2,
             shift_id,
             line_id,
             start_org_id,
             start_org_name,
             end_org_id,
             end_org_name,
             order_num,
             driver1_emp_id,
             concat(substr(driver1_name, 1, 1), '*')  driver1_name,
             driver2_emp_id,
             concat(substr(driver2_name, 1, 1), '*')  driver2_name,
             truck_id,
             md5(truck_no)     truck_no,
             date_format(from_utc_timestamp(cast(actual_start_time as bigint), 'UTC'), 'yyyy-MM-dd HH:mm:ss')    actual_start_time,
             date_format(from_utc_timestamp( cast(actual_end_time as bigint), 'UTC'),'yyyy-MM-dd HH:mm:ss')       actual_end_time,
             actual_distance,
             (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec,
             dt,
             date_format(from_utc_timestamp( cast(actual_end_time as bigint), 'UTC'),'yyyy-MM-dd')  dt1
      from ods_transport_task
      where dt = '2025-07-13'
        and is_deleted = '0'
        and actual_end_time is not null) info
         left join
     (select id id3,
             estimated_time
      from dim_shift_full
      where dt = '2025-07-13') dim_tb
     on info.shift_id = dim_tb.id3;

-- 10.2 每日装载
insert overwrite table dwd_trans_trans_finish_inc
    partition (dt = '2025-07-13')
select info.id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       driver1_name,
       driver2_emp_id,
       driver2_name,
       truck_id,
       truck_no,
       actual_start_time,
       actual_end_time,
       estimated_time estimate_end_time,
       actual_distance,
       finish_dur_sec,
     dt
from (select id,
       shift_id,
       line_id,
       start_org_id,
       start_org_name,
       end_org_id,
       end_org_name,
       order_num,
       driver1_emp_id,
       concat(substr(driver1_name, 1, 1), '*')                                            driver1_name,
       driver2_emp_id,
       concat(substr(driver2_name, 1, 1), '*')                                            driver2_name,
       truck_id,
       md5(truck_no)                                                                      truck_no,
       date_format(from_utc_timestamp(
                           cast(actual_start_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')                                                       actual_start_time,
       date_format(from_utc_timestamp(
                           cast(actual_end_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss')                                                       actual_end_time,
       actual_distance,
       (cast(actual_end_time as bigint) - cast(actual_start_time as bigint)) / 1000 finish_dur_sec,
     dt                                                                                     dt
from ods_transport_task
where dt = '2025-07-13'
  
  and actual_end_time is null
  and actual_end_time is not null
  and is_deleted = '0') info
         left join
     (select id,
             estimated_time
      from dim_shift_full
      where dt = '2025-07-13') dim_tb
     on info.shift_id = dim_tb.id;

-- 11. dwd_bound_inbound_inc
-- 11.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_inbound_inc
    partition (dt)
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') inbound_time,
       inbound_emp_id,
       date_format(from_utc_timestamp(
                           cast(inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound
where dt = '2025-07-13';

-- 11.2 每日装载
insert overwrite table dwd_bound_inbound_inc
    partition (dt = '2025-07-13')
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(inbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') inbound_time,
       inbound_emp_id
from ods_order_org_bound
where dt = '2025-07-13'
  ;

-- 12. dwd_bound_sort_inc
-- 12.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_sort_inc
    partition (dt)
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') sort_time,
       sorter_emp_id,
       date_format(from_utc_timestamp(
                           cast(sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound
where dt = '2025-07-13'
  and sort_time is not null;

-- 12.2 每日装载
insert overwrite table dwd_bound_sort_inc
    partition (dt = '2025-07-13')
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(sort_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') sort_time,
       sorter_emp_id
from ods_order_org_bound
where dt = '2025-07-13'
  
  and sort_time is null
  and sort_time is not null
  and is_deleted = '0';

-- 13. dwd_bound_outbound_inc
-- 13.1 首日装载
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_bound_outbound_inc
    partition (dt)
select id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') outbound_time,
       outbound_emp_id,
       date_format(from_utc_timestamp(
                           cast(outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd')          dt
from ods_order_org_bound
where dt = '2025-07-13'
  and outbound_time is not null;

-- 13.2 每日装载
insert overwrite table dwd_bound_outbound_inc
    partition (dt = '2025-07-13')
select  id,
       order_id,
       org_id,
       date_format(from_utc_timestamp(
                           cast(outbound_time as bigint), 'UTC'),
                   'yyyy-MM-dd HH:mm:ss') outbound_time,
       outbound_emp_id
from ods_order_org_bound
where dt = '2025-07-13'
  
  and outbound_time is null
  and outbound_time is not null
  and is_deleted = '0';
