FROM python:3.14.7-trixie

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libc6-dev \
    python3-dev \
    libmariadb-dev \
    linux-libc-dev \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE ${HOOTER_APPLICATION_PORT}

CMD ["python", "app.py"]