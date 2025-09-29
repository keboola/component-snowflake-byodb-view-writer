FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# install gcc and required libraries
RUN apt-get update && apt-get install -y \
    git \
    libssl-dev \
    openssl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code/

COPY pyproject.toml .
COPY uv.lock .

ENV UV_PROJECT_ENVIRONMENT="/usr/local/"
RUN uv sync --all-groups --frozen

COPY src/ src
COPY tests/ tests
COPY scripts/ scripts
COPY flake8.cfg .
COPY deploy.sh .

CMD ["python", "-u", "src/component.py"]
