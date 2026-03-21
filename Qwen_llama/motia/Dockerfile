FROM debian:bookworm-slim

# curl and ca-certificates were missing — iii/uv install scripts need them
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates bash supervisor \
    && rm -rf /var/lib/apt/lists/*

# Install iii engine + console binaries
RUN curl -fsSL https://install.iii.dev/iii/main/install.sh | bash
RUN curl -fsSL https://install.iii.dev/console/main/install.sh | bash

ENV PATH="/root/.local/bin:$PATH"

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | bash

WORKDIR /app/motia

# Copy dependency files first (layer caching)
COPY motia/pyproject.toml motia/uv.lock ./

# Install Python dependencies
RUN /root/.local/bin/uv sync

# Copy entire project
COPY . /app/

# Copy supervisord config — this was the missing step
COPY motia/supervisord.conf /etc/supervisor/conf.d/motia.conf

# Expose all ports including 3113 for the console UI
EXPOSE 3113 3121 3124 49134

# Use supervisord to run BOTH iii engine + iii-console together
CMD ["supervisord", "-n", "-c", "/etc/supervisor/conf.d/motia.conf"]