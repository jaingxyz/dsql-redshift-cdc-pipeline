# Container image for the order_simulator running as an always-on Fargate
# task. Built for linux/arm64 (Graviton Fargate is ~20% cheaper than x86).
#
# Build + push (one-time, from a Mac with docker):
#   aws ecr get-login-password --region us-east-1 \
#     | docker login --username AWS --password-stdin <acct>.dkr.ecr.us-east-1.amazonaws.com
#   docker buildx build --platform linux/arm64 \
#     -t <acct>.dkr.ecr.us-east-1.amazonaws.com/dsql-cdc-simulator:latest \
#     --push app/
#
# scripts/05-deploy-simulator.sh wraps the above. The Dockerfile lives in
# app/ so the build context is just the simulator code (not the whole repo).

FROM --platform=linux/arm64 python:3.11-slim

# psycopg[binary] needs libpq at runtime; the binary wheel bundles it but
# also needs libssl + libsasl. python:3.11-slim already has libssl.
# (If we ever switch to psycopg[c] from source, add: libpq-dev gcc.)

WORKDIR /app

# Copy requirements first so dependency installs cache between code edits.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY order_simulator.py .

# Run forever at 1 TPS, reconnecting on token expiry. The Fargate task
# definition can override --rate via command override if you want to
# change throughput without rebuilding the image.
CMD ["python", "order_simulator.py", "--duration", "0", "--rate", "1"]
