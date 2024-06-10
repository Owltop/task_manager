FROM python:3.9

COPY __init__.py __init__.py

COPY backend_registration/ /backend_registration
COPY proto/ proto/

RUN pip install -r backend_registration/requirements.txt
ENV PYTHONPATH "../"
CMD ["python3", "backend_registration/registration.py"]