# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/devcontainers/python:3.11

# Install Python dependencies at build time
COPY requirements.txt /tmp/pip-tmp/
RUN pip install --no-cache-dir -r /tmp/pip-tmp/requirements.txt \
    && rm -rf /tmp/pip-tmp

WORKDIR /workspace
