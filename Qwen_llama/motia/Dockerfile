FROM debian:bookworm-slim

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates bash\
    && rm -rf /var/lib/apt/lists/*

# Install latest iii engine (Linux binary - works perfectly in Docker)
RUN curl -fsSL https://install.iii.dev/iii/main/install.sh | bash
RUN curl -fsSL https://install.iii.dev/console/main/install.sh | bash

ENV PATH="/root/.local/bin:$PATH"

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | bash

WORKDIR /app/motia

# Copy dependency files first (for layer caching)
COPY motia/pyproject.toml motia/uv.lock ./

# Install Python dependencies
RUN /root/.local/bin/uv sync

# Copy entire project
COPY . /app/

EXPOSE 3121 3124 49134

CMD ["iii", "-c", "iii-config.yaml"]