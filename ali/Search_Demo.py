# -*- coding: utf-8 -*-

import time
from typing import Dict, Any

from Tea.exceptions import TeaException
from Tea.request import TeaRequest
from alibabacloud_tea_util import models as util_models

from ali_BaseRequest import Config, Client


class opensearch:
    def __init__(self, config: Config):
        self.Clients = Client(config=config)
        self.runtime = util_models.RuntimeOptions(
            connect_timeout=10000,
            read_timeout=10000,
            autoretry=False,
            ignore_ssl=False,
            max_idle_conns=50,
            max_attempts=3
        )
        self.header = {}

    def searchDoc(self, app_name: str, query_params: dict) -> Dict[str, Any]:
        try:
            response = self.Clients._request(method="GET", pathname=f'/v3/openapi/apps/{app_name}/search',
                                             query=query_params, headers=self.header, body=None, runtime=self.runtime)
            return response
        except TeaException as e:
            print(e)


if __name__ == "__main__":
    # 配置统一的请求入口和  需要去掉http://
    endpoint = "opensearch-cn-zhangjiakou.aliyuncs.com"

    # 支持 protocol 配置 HTTPS/HTTP
    endpoint_protocol = "HTTP"

    # 用户识别信息
    access_key_id = "LTAI5tMkGc5wgVGHRcJQLDFG"
    access_key_secret = "o2yOYWcpKjgC4oF8Z3MRgWADQfR41m"

    # 支持 type 配置 sts/access_key 鉴权. 其中 type 默认为 access_key 鉴权. 使用 sts 可配置 RAM-STS 鉴权.
    # 备选参数为:  sts 或者 access_key
    auth_type = "access_key"

    # 配置请求使用的通用信息.
    # type和security_token 参数如果不是子账号，需要省略
    Configs = Config(endpoint=endpoint, access_key_id=access_key_id, access_key_secret=access_key_secret,
                     type=auth_type, protocol=endpoint_protocol)

    # 创建 opensearch 实例
    ops = opensearch(Configs)
    # OpenSearch应用名
    app_name = "search"

    # --------------- 文档搜索 ---------------

    docQuery = {
        "query": "config=start:0,hit:10,format:fulljson&&query=(default:\'词典\')"
    }

    res1 = ops.searchDoc(app_name=app_name, query_params=docQuery)
    print(res1)