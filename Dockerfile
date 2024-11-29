# syntax=docker/dockerfile:1

FROM python:3.10-slim
WORKDIR /webapp-server
COPY . .
RUN if [ -f ".venv/bin/python" ]; then \
  source .venv/bin/activate; \
  fi
RUN pip3 install -r requirements.txt
CMD ["gunicorn", "--workers=2", "--bind=0.0.0.0:8000", "app:app"]
