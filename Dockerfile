FROM python:3.14-alpine3.24

WORKDIR /app

RUN apk add --no-cache linux-headers gcc musl-dev mariadb-dev

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE ${HOOTER_APPLICATION_PORT}

CMD ["python", "app.py"]