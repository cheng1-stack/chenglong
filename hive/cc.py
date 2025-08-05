import pandas as pd
import pyhive.hive
import pymysql
import logging
from datetime import datetime
import configparser

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def read_config(config_path='config.ini'):
    """读取配置文件"""
    config = configparser.ConfigParser()
    config.read(config_path)

    return {
        "mysql": {
            "host": config.get('mysql', 'host'),
            "port": config.getint('mysql', 'port'),
            "user": config.get('mysql', 'user'),
            "password": config.get('mysql', 'password'),
            "database": config.get('mysql', 'database'),
            "table": config.get('mysql', 'table')
        },
        "hive": {
            "host": config.get('hive', 'host'),
            "port": config.getint('hive', 'port'),
            "database": config.get('hive', 'database'),
            "table": config.get('hive', 'table'),
            "location": config.get('hive', 'location')
        },
        "batch_size": config.getint('sync', 'batch_size'),
        "dt": config.get('sync', 'dt', fallback=datetime.now().strftime('%Y%m%d'))
    }


def get_mysql_table_structure(conn, table_name):
    """获取MySQL表结构"""
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [column[0] for column in cursor.fetchall()]
    cursor.close()
    return columns


def create_hive_table_and_partition(hive_conn, config, mysql_columns):
    """创建Hive表和分区"""
    cursor = hive_conn.cursor()
    dt = config['dt']
    hive_db = config['hive']['database']
    hive_table = config['hive']['table']

    # 创建数据库
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {hive_db}")
    cursor.execute(f"USE {hive_db}")
    logger.info(f"USE `{hive_db}`")

    # 构建创建表SQL（根据MySQL字段类型映射Hive类型）
    # 这里简化处理，实际应根据具体类型映射
    columns_def = []
    for col in mysql_columns:
        if col in ['from_page_id', 'to_page_id']:
            columns_def.append(f"`{col}` BIGINT COMMENT '{col}'")
        elif col == 'relation_type':
            columns_def.append(f"`{col}` STRING COMMENT '{col}'")
        elif col == 'create_time':
            columns_def.append(f"`{col}` TIMESTAMP COMMENT '{col}'")

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS `{hive_table}` (
        {', '.join(columns_def)}
    )
    COMMENT 'MySQL同步表（含分区）'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '{config['hive']['location']}'
    """
    logger.info(create_table_sql.strip())
    cursor.execute(create_table_sql)

    # 添加分区
    add_partition_sql = f"""
    ALTER TABLE `{hive_table}`
    ADD IF NOT EXISTS PARTITION(dt='{dt}')
    """
    logger.info(add_partition_sql.strip())
    cursor.execute(add_partition_sql)

    cursor.close()
    logger.info(f"创建Hive表及分区 dt={dt} 成功")


def read_and_clean_mysql_data(mysql_conn, config):
    """从MySQL读取并清洗数据"""
    table_name = config['mysql']['table']
    logger.info(f"从MySQL读取表: {table_name}")

    # 使用pandas读取数据（建议使用SQLAlchemy连接）
    df = pd.read_sql(f"SELECT * FROM {table_name}", mysql_conn)
    logger.info(f"从MySQL读取 {len(df)} 条数据")

    # 清洗空值
    df = df.dropna()
    logger.info(f"过滤空值后剩余 {len(df)} 条数据")

    # 添加分区字段
    df['dt'] = config['dt']
    return df


def write_to_hive_partition(hive_conn, df, config):
    """批量写入Hive分区"""
    cursor = hive_conn.cursor()
    dt = config['dt']
    hive_db = config['hive']['database']
    hive_table = config['hive']['table']
    batch_size = config['batch_size']

    cursor.execute(f"USE {hive_db}")
    logger.info(f"USE `{hive_db}`")

    # 获取列名（包含dt分区字段）
    columns = df.columns.tolist()
    columns_str = ', '.join([f'`{col}`' for col in columns])

    # 批量插入数据
    total_rows = len(df)
    for i in range(0, total_rows, batch_size):
        batch = df.iloc[i:i + batch_size]
        values = []

        for _, row in batch.iterrows():
            row_values = []
            for col in columns:
                if pd.isna(row[col]):
                    row_values.append('NULL')
                elif col in ['from_page_id', 'to_page_id']:
                    row_values.append(str(row[col]))
                elif col == 'create_time':
                    # 处理时间格式
                    row_values.append(f"'{row[col].strftime('%Y-%m-%d %H:%M:%S')}'")
                else:
                    # 字符串类型需要加引号
                    row_values.append(f"'{str(row[col]).replace("'", "\\'")}'")

            values.append(f"({', '.join(row_values)})")

        insert_sql = f"""
        INSERT INTO {hive_db}.{hive_table} ({columns_str})
        VALUES {', '.join(values)}
        """

        try:
            cursor.execute(insert_sql)
            logger.info(f"已写入 {min(i + batch_size, total_rows)}/{total_rows} 条数据")
        except Exception as e:
            logger.error(f"写入Hive失败: {str(e)}")
            raise

    cursor.close()
    logger.info(f"所有数据写入完成，共 {total_rows} 条")


def main():
    try:
        # 读取配置
        config = read_config()

        # 连接MySQL
        mysql_conn = pymysql.connect(
            host=config['mysql']['host'],
            port=config['mysql']['port'],
            user=config['mysql']['user'],
            password=config['mysql']['password'],
            database=config['mysql']['database']
        )

        # 连接Hive
        hive_conn = pyhive.hive.connect(
            host=config['hive']['host'],
            port=config['hive']['port']
        )

        logger.info("===== 开始MySQL到Hive分区同步 =====")

        # 获取MySQL表结构
        mysql_columns = get_mysql_table_structure(mysql_conn, config['mysql']['table'])
        logger.info(f"获取MySQL表结构成功，共 {len(mysql_columns)} 个字段")

        # 创建Hive表和分区
        create_hive_table_and_partition(hive_conn, config, mysql_columns)

        # 读取并清洗数据
        cleaned_df = read_and_clean_mysql_data(mysql_conn, config)

        # 写入Hive
        write_to_hive_partition(hive_conn, cleaned_df, config)

    except Exception as e:
        logger.error(f"同步失败: {str(e)}", exc_info=True)
    finally:
        if 'mysql_conn' in locals() and mysql_conn.open:
            mysql_conn.close()
        if 'hive_conn' in locals() and not hive_conn.closed:
            hive_conn.close()
        logger.info("===== 同步结束 =====")


if __name__ == "__main__":
    main()