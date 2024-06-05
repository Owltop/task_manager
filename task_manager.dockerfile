FROM python:3.8-slim

COPY __init__.py __init__.py
COPY task_manager/ task_manager/
COPY proto/ proto/
COPY zalupa.py zalupa.py
RUN pip install -r task_manager/requirements.txt
ENV PYTHONPATH "../"
ENTRYPOINT ["python3", "./kek.py"]