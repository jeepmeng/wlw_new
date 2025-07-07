def vector_to_pgstring(vec: list[float]) -> str:
    return "[" + ",".join(map(str, vec)) + "]"



# def vector_to_pgstring(vec: list[float]) -> str:
#     """将向量安全转为 PostgreSQL json/数组字符串"""
#     return "[" + ",".join(f"{float(v):.6f}" for v in vec) + "]"