FROM python:3.9

COPY __init__.py __init__.py

COPY task_manager/ task_manager/
COPY proto/ proto/
COPY kek.py kek.py

RUN pip install -r task_manager/requirements.txt
ENV PYTHONPATH "../"
ENTRYPOINT ["python3", "task_manager/task_manager.py"]