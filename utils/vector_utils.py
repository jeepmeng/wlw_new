def vector_to_pgstring(vec: list[float]) -> str:
    return "[" + ",".join(map(str, vec)) + "]"