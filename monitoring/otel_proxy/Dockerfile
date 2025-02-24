FROM python:latest

RUN mkdir /opt/otlp_proxy
WORKDIR /opt/otlp_proxy
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY . .

CMD ["python3", "-m", "otlp_proxy"]