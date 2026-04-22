# Multi-stage: build the Astro site, then serve it from a minimal Caddy.
# The outer edge Caddy (see Caddyfile) reverse-proxies to this container for
# anything that isn't /api/*.

FROM node:20-alpine AS build
WORKDIR /web

COPY web/package.json web/package-lock.json* ./
RUN npm install --no-audit --no-fund --loglevel=warn

COPY web/ ./
RUN npm run build

FROM caddy:2-alpine AS runtime
COPY --from=build /web/dist /usr/share/caddy

# Minimal inline Caddyfile: serve static files, set sensible cache headers.
# SPA-style fallback is not needed (Astro emits a real file per route).
RUN cat > /etc/caddy/Caddyfile <<'EOF'
:80 {
    root * /usr/share/caddy
    encode gzip zstd

    # Hashed assets can be cached forever; HTML should revalidate.
    @immutable path /_assets/*
    header @immutable Cache-Control "public, max-age=31536000, immutable"

    @html path *.html /
    header @html Cache-Control "no-cache"

    file_server
}
EOF

EXPOSE 80
