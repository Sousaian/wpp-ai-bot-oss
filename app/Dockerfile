FROM python:3.12-slim@sha256:e8a38829209aea82774882e5157e1f7c3e8c8393ff53a697b3580896e57f345c

WORKDIR /app

# All locked dependencies have CPython 3.12 linux/amd64 wheels; no apt toolchain.
COPY requirements.lock .
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r requirements.lock

COPY . .

ARG DELIVERY_WORKER_CODE_VERSION
RUN python -c 'import os,re; from pathlib import Path; v=os.environ.get("DELIVERY_WORKER_CODE_VERSION", ""); assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,99}", v), "image_code_version_required"; p=Path("/etc/wpp-app"); p.mkdir(parents=True); (p/"IMAGE_CODE_VERSION").write_text(v+"\n", encoding="ascii")'
LABEL org.opencontainers.image.revision=$DELIVERY_WORKER_CODE_VERSION

EXPOSE 5000

CMD ["python", "-m", "src.app_server"]
