import logging
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
from dotenv import load_dotenv
import re
import time



# 加载 .env 文件中的变量
load_dotenv()


def clean_old_logs(log_dir, name, keep_days):
    now = time.time()
    pattern = re.compile(rf"{re.escape(name)}_.*_(\d{{4}}-\d{{2}}-\d{{2}})\.log")

    for fname in os.listdir(log_dir):
        match = pattern.match(fname)
        if match:
            date_str = match.group(1)
            try:
                file_time = datetime.strptime(date_str, "%Y-%m-%d").timestamp()
                if now - file_time > keep_days * 86400:
                    os.remove(os.path.join(log_dir, fname))
            except Exception as e:
                pass  # 可加日志记录异常


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

    clean_old_logs(log_dir, name, backup_count)
    return logger