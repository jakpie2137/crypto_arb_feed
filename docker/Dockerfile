# ============================================================
# Trading Platform - Multi-service Docker Image
# ============================================================
# Usage:
#   docker build -f docker/Dockerfile -t trading-platform .
#   docker run -e APP_TYPE=feed -e OB_GUI=false trading-platform
#   docker run -e APP_TYPE=router trading-platform
#   docker run -e APP_TYPE=mm -e MM_CONFIG_PATH=conf/mm/ml_bitget_01.yaml trading-platform
# ============================================================

# ============================================================
# STAGE 1: BUILD
# ============================================================
# Use glibc-based image: protoc-gen-grpc-java binaries require glibc (Alpine/musl incompatible)
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /build

# Copy only files needed for dependency resolution (better caching)
COPY gradlew gradlew.bat ./
COPY gradle/ gradle/
COPY build.gradle.kts settings.gradle.kts gradle.properties ./
COPY java-platform/build.gradle.kts java-platform/

# Download dependencies (cached layer)
RUN chmod +x gradlew && ./gradlew dependencies --no-daemon || true

# Copy source code
COPY java-platform/src java-platform/src

# Build distribution (JAR + dependencies)
RUN ./gradlew :java-platform:installDist -x test --no-daemon

# ============================================================
# STAGE 2: RUNTIME
# ============================================================
FROM eclipse-temurin:21-jre-alpine

LABEL maintainer="trading-team"
LABEL description="crypto_arb_feed - WebSocket feeds + feed-proxy"

WORKDIR /app

# Install curl for healthchecks
RUN apk add --no-cache curl

# Copy distribution (JAR + all dependencies)
COPY --from=builder /build/java-platform/build/install/java-platform/lib/*.jar ./lib/

# Copy configurations
COPY conf/ ./conf/

# ============================================================
# Environment Variables (defaults)
# ============================================================
# JVM settings
ENV JAVA_OPTS="-Xms256m -Xmx1g -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError"

# Application type: feed, mm, router, reporter
ENV APP_TYPE=feed

# GUI (always false in container)
ENV OB_GUI=false

# Database connection
ENV ARB_PG_URL=jdbc:postgresql://postgres:5432/arb_test
ENV ARB_PG_USER=postgres
# Password: loaded automatically from Docker secret /run/secrets/pg_password
# Fallback: /opt/trading/secrets/pg_password.txt or ARB_PG_PASS env

# Router connection (for MM). Set via docker-compose per service.
# Port 5050 to avoid conflict with macOS AirPlay on 5000.
ENV ROUTER_PORT=5050

# Health endpoint
ENV HEALTH_PORT=8080

# MM config path (for APP_TYPE=mm)
ENV MM_CONFIG_PATH=conf/mm/ml_bitget_01.yaml

# ============================================================
# Ports
# ============================================================
# 8080: health + /prices (Feed) | 5050: router | 50050: Feed gRPC (OrderbookUpdate stream)
EXPOSE 8080 5050 50050

# ============================================================
# Healthcheck
# ============================================================
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -f http://localhost:${HEALTH_PORT}/health 2>/dev/null || exit 1

# ============================================================
# Entrypoint - selects main class based on APP_TYPE
# ============================================================
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
