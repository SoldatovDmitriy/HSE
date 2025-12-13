import os
import time
import sys
import logging
import signal
from contextlib import contextmanager
import psycopg2
from psycopg2 import sql, OperationalError, DatabaseError

# --- Конфигурация из переменных окружения ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
PING_INTERVAL = int(os.getenv("PING_INTERVAL_SECONDS", str(5 * 60)))  # по умолчанию 300
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT_SECONDS", "10"))  # timeout для подключения
LOG_FILE = os.getenv("LOG_FILE", "")  # если указан — дублируем логи в файл

if not DB_USER or not DB_PASS:
    print("ERROR: DB_USER and DB_PASS environment variables must be set.", file=sys.stderr)
    sys.exit(2)

# --- Логирование: stdout/stderr и (опционально) файл ---
logger = logging.getLogger("pinger")
logger.setLevel(logging.DEBUG)

# stdout handler (INFO+)
stdout_h = logging.StreamHandler(sys.stdout)
stdout_h.setLevel(logging.INFO)
stdout_h.setFormatter(logging.Formatter("%(asctime)s [INFO] %(message)s"))
logger.addHandler(stdout_h)

# stderr handler (WARNING+)
stderr_h = logging.StreamHandler(sys.stderr)
stderr_h.setLevel(logging.WARNING)
stderr_h.setFormatter(logging.Formatter("%(asctime)s [ERROR] %(message)s"))
logger.addHandler(stderr_h)

# file handler (if needed) — append mode
if LOG_FILE:
    os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
    file_h = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    file_h.setLevel(logging.DEBUG)
    file_h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(file_h)

# --- Graceful shutdown ---
stop_requested = False


def _signal_handler(signum, frame):
    global stop_requested
    stop_requested = True
    logger.info(f"Signal {signum} received: stopping after current iteration.")


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


@contextmanager
def connect_with_timeout(**kwargs):
    """
    Контекстный менеджер для подключения с connect_timeout.
    Устанавливаем statement_timeout на короткое время, чтобы execute не висел.
    """
    conn = None
    try:
        conn = psycopg2.connect(connect_timeout=CONNECT_TIMEOUT, **kwargs)
        # Установим небольшую задержку для выполнения запросов (ms).
        # Если СУБД поддерживает параметр statement_timeout, выставим его.
        with conn.cursor() as cur:
            try:
                cur.execute("SET statement_timeout = %s;", (int(CONNECT_TIMEOUT * 1000),))
                conn.commit()
            except Exception:
                # Если СУБД не поддерживает / не разрешает — игнорируем
                conn.rollback()
        yield conn
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def check_version():
    """Попытка подключиться и получить SELECT VERSION();"""
    try:
        with connect_with_timeout(
            dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT VERSION();")
                row = cur.fetchone()
                if not row:
                    logger.warning("Connected but VERSION() returned empty result.")
                    return
                version = str(row[0])
                # типичный ответ PostgreSQL обычно начинается с 'PostgreSQL'
                if version.startswith("PostgreSQL"):
                    logger.info(f"Successful connection. VERSION: {version}")
                else:
                    # атипичный, но не ошибочный ответ — записываем в stdout
                    logger.info(f"Atypical VERSION response (no error): {version}")
    except OperationalError as e:
        # ошибки подключения/аутентификации и т.п.
        logger.error(f"Connection failed: {e}")
    except DatabaseError as e:
        # ошибки выполнения запроса
        logger.error(f"Database error: {e}")
    except Exception as e:
        # защита от неожиданных исключений
        logger.error(f"Unexpected error: {e}")


def main_loop():
    logger.info("pinger started. Configuration: host=%s port=%s db=%s interval=%s",
                DB_HOST, DB_PORT, DB_NAME, PING_INTERVAL)
    while not stop_requested:
        start = time.time()
        try:
            check_version()
        except Exception as e:
            logger.error(f"Unhandled error in check_version: {e}")
        # Вычисляем оставшееся время до следующего опроса, учитывая время выполнения
        elapsed = time.time() - start
        to_sleep = max(0, PING_INTERVAL - elapsed)
        # Но не блокируем на очень длинное время если уже стоп запрошен
        if to_sleep > 0:
            # sleep маленькими шагами, чтобы корректно прерываться сигналом
            step = 1.0
            while to_sleep > 0 and not stop_requested:
                time.sleep(min(step, to_sleep))
                to_sleep -= step
    logger.info("pinger stopped.")


if __name__ == "__main__":
    try:
        main_loop()
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
