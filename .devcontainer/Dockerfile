# Use a tested devcontainer python image
FROM mcr.microsoft.com/devcontainers/python:1-3.11

# Create workspace dir (matches devcontainer.json)
WORKDIR /workspace

# Copy requirements if present at build time (speeds up later startup)
COPY requirements.txt /workspace/requirements.txt

# Install requirements (non-root user available as "vscode")
RUN --mount=type=cache,target=/root/.cache \
    pip install --no-cache-dir -r /workspace/requirements.txt || true

# Ensure the vscode user owns the workspace
USER vscode
