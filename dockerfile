FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9

ADD ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt
WORKDIR /app

ADD ./src  /app

ENV PYTHONPATH=/app:$PYTHONPATH

EXPOSE 4151

CMD ['python', 'server.py']