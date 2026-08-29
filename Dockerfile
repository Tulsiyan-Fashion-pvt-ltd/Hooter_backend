FROM fedora:44

WORKDIR /app

RUN dnf install -y python3.14 python3.14-pip python3-devel gcc mariadb-connector-c-devel

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

EXPOSE ${HOOTER_APPLICATION_PORT}

CMD ["python", "app.py"]