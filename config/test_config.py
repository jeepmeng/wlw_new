from config import settings

def main():
    print("当前环境：", settings.env)
    print("数据库地址：", settings.wmx_database.DB_HOST)
    print("DB_PORT：", settings.wmx_database.DB_PORT)
    # print("向量接口地址：", settings.api_urls.vector_api)

if __name__ == "__main__":
    main()