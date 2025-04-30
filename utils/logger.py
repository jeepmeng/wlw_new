import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from dotenv import load_dotenv

# 加载 .env 文件中的变量
load_dotenv()

def setup_logger(name: str, log_dir="logs", when="midnight", backup_count=7):
    os.makedirs(log_dir, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")

    env = os.getenv("ENV", "dev")  # 默认为 dev

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if env == "dev" else logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - [%(name)s] - %(message)s")

    # ✅ 控制台日志级别根据环境切换
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if env == "dev" else logging.WARNING)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # ✅ info 文件
    info_file = os.path.join(log_dir, f"{name}_info_{date_str}.log")
    info_handler = TimedRotatingFileHandler(info_file, when=when, interval=1, backupCount=backup_count, encoding="utf-8")
    info_handler.setLevel(logging.INFO)
    info_handler.setFormatter(formatter)
    logger.addHandler(info_handler)

    # ✅ error 文件
    error_file = os.path.join(log_dir, f"{name}_error_{date_str}.log")
    error_handler = TimedRotatingFileHandler(error_file, when=when, interval=1, backupCount=backup_count, encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)

    return logger