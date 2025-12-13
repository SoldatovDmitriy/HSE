# Используем официальный Python slim
FROM python:3.12-slim

# Рабочая директория
WORKDIR /pinger

# Копируем requirements и ставим
COPY requirements.txt /pinger/
RUN pip install --no-cache-dir -r requirements.txt

# Копируем приложение
COPY main.py /pinger/
# Если есть дополнительные файлы (например, конфиг) — копируем тоже
# COPY postgresql.conf /app/postgresql.conf

# Убедимся, что файл исполняемый
RUN chmod +x /pinger/main.py

# По умолчанию переменные окружения можно переопределить через docker-compose / -e
ENV PING_INTERVAL_SECONDS=300
ENV CONNECT_TIMEOUT_SECONDS=10

CMD ["python", "/pinger/main.py"]
