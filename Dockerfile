FROM --platform=linux/amd64 debian:trixie-slim AS build

WORKDIR /src

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Zig, pinned to the version declared in build.zig.zon.
ARG ZIG_VERSION=0.16.0
RUN curl -sSfL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /opt \
    && ln -s "/opt/zig-x86_64-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig

# Build the Wasm module and the API docs. The Zig build fetches the `ordered`
# dependency declared in build.zig.zon, so this layer needs network access.
COPY build.zig build.zig.zon ./
COPY src/ src/
COPY web/zodd_wasm.zig web/zodd_wasm.zig
RUN zig build wasm docs

# Pinned to a specific minor for reproducibility; bump deliberately when needed.
FROM nginx:1.27-alpine

# The same assets the docs workflow deploys: the playground at the site root
# and the API docs under /api.
COPY web/index.html web/main.js web/style.css logo.svg /usr/share/nginx/html/
COPY --from=build /src/zig-out/web/zodd.wasm /usr/share/nginx/html/zodd.wasm
COPY --from=build /src/zig-out/docs/ /usr/share/nginx/html/api/

# Pre-compress static assets so nginx can serve them via gzip_static. Keeps the
# original alongside (-k) so clients without gzip support still work.
RUN find /usr/share/nginx/html -type f \( \
        -name "*.wasm" -o -name "*.js" -o -name "*.html" -o -name "*.css" \
        -o -name "*.svg" -o -name "*.tar" \
    \) -exec gzip -9 -k -f {} \;

RUN printf '%s\n' \
    'server {' \
    '    listen 80;' \
    '    server_name _;' \
    '    root /usr/share/nginx/html;' \
    '    index index.html;' \
    '    gzip_static on;' \
    '    location / { try_files $uri $uri/ =404; }' \
    '}' > /etc/nginx/conf.d/default.conf

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --spider -q http://localhost/ || exit 1

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
