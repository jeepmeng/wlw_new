import logging
import os
import re
import time
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from config.settings import ENV

_loggers = {}  # 全局 logger 缓存，防止重复初始化


def get_logger(
    name: str,
    log_dir: str = "logs",
    keep_days: int = 7,
    when: str = "midnight"
) -> logging.Logger:
    """
    获取或初始化一个 logger。

    :param name: 日志名称（模块名）
    :param log_dir: 日志存储目录
    :param keep_days: 保留天数
    :param when: 日志轮转频率（默认按天）
    :param env_var: 环境变量名，用于控制日志级别（默认 ENV）
    :return: logger 实例
    """
    if name in _loggers:
        return _loggers[name]

    os.makedirs(log_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")


    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if ENV == "dev" else logging.INFO)


    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - [%(name)s] - %(message)s"
    )

    # 控制台输出
    console_handler = logging.StreamHandler()
    # console_level = logging.DEBUG if env == "dev" else logging.WARNING
    console_level = logging.DEBUG if ENV == "dev" else logging.WARNING
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 错误日志文件
    error_file = os.path.join(log_dir, f"{name}_error_{date_str}.log")
    error_handler = TimedRotatingFileHandler(
        error_file, when=when, interval=1, backupCount=keep_days, encoding="utf-8"
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    # 清理旧日志（仅 error 文件，按文件名日期）
    _clean_old_logs(log_dir, name, keep_days)

    logger.debug(f"[logger] 当前 ENV: {ENV}")

    _loggers[name] = logger
    return logger


def _clean_old_logs(log_dir: str, name: str, keep_days: int):
    now = time.time()
    pattern = re.compile(rf"{re.escape(name)}_error_(\d{{4}}-\d{{2}}-\d{{2}})\.log")

    for fname in os.listdir(log_dir):
        match = pattern.match(fname)
        if match:
            try:
                log_date = datetime.strptime(match.group(1), "%Y-%m-%d").timestamp()
                if now - log_date > keep_days * 86400:
                    os.remove(os.path.join(log_dir, fname))
            except Exception:
                continue