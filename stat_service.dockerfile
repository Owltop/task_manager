FROM python:3.9

COPY __init__.py __init__.py

COPY stat_service/ stat_service/
COPY proto/ proto/

RUN pip install -r stat_service/requirements.txt
ENV PYTHONPATH "../"
CMD ["python3", "stat_service/stat_service.py"]
