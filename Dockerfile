FROM python:3.11-slim

# Speed up installs and avoid building from source whenever possible
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ONLY_BINARY=:all: \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

COPY src/ /app/src/
COPY data/ /app/data/

# Default command runs the main logic directly
CMD ["python","-u","-m","src.lambda_function"]
