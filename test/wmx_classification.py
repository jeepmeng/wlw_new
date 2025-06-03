import psycopg2  # 如果是PostgreSQL，其他数据库请替换相应的库
from typing import Dict


def get_dict_data(db_params: dict) -> Dict[str, str]:
    """
    从sys_dict_data表中获取dict_label和dict_value并返回为字典

    :param db_params: 数据库连接参数
    :return: 包含dict_label: dict_value的字典
    """
    query = """
    SELECT dict_label, dict_value 
    FROM sys_dict_data 
    WHERE dict_type = 'industry_type'
    """

    result_dict = {}

    try:
        # 连接数据库（这里以PostgreSQL为例）
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            dict_label, dict_value = row
            result_dict[dict_label] = dict_value

    except Exception as e:
        print(f"数据库操作出错: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

    return result_dict


# 使用示例
if __name__ == "__main__":
    # 替换为你的数据库连接参数
    db_params = {
        'host': '172.25.232.238',
        'database': 'wmx',
        'user': 'root',
        'password': 'szqy@CC1243!',
        'port': '5432'
    }

    industry_dict = get_dict_data(db_params)
    print(industry_dict)