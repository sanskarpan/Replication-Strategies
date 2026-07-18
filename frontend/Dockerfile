# syntax=docker/dockerfile:1

# Bun-based frontend BFF. Serves the browser bundle and proxies /api and /ws to
# the Go backend (see server/bff.ts, which reads process.env.BACKEND).
FROM oven/bun:1

WORKDIR /app

# URL of the Go backend, consumed by server/bff.ts at runtime. Overridable at
# build time (--build-arg BACKEND=...) or runtime (-e BACKEND=...).
ARG BACKEND=http://backend:8080
ENV BACKEND=${BACKEND}

# Install dependencies first for better layer caching. --frozen-lockfile keeps
# the resolved tree in sync with bun.lock.
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# Application source.
COPY . .

EXPOSE 3001

CMD ["bun", "server/bff.ts"]
