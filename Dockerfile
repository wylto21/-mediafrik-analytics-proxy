FROM node:22-alpine

# No dependencies to install — the proxy uses only Node built-ins, so there is
# no package.json, no lockfile and no npm install step to keep in sync.
WORKDIR /app
COPY analytics-proxy.js analytics-store.js ./

# The local store lives here. Mount a volume on it, or every deploy starts the
# site's analytics history from scratch.
RUN mkdir -p /data && chown node:node /data
VOLUME /data

# Listen on all interfaces: inside a container the reverse proxy reaches this
# over the Docker network, not over loopback.
ENV BIND=0.0.0.0 \
    PORT=3000 \
    NODE_ENV=production

# Secrets and tunables come from the environment (TB_TRACKER_TOKEN,
# SESSION_SECRET, ALLOWED_ORIGIN, TRUSTED_PROXY_CIDRS, XFF_TRUST_HOPS).
# SESSION_SECRET matters: without it the container generates one at boot and
# every deploy would reset visitor sessions.

USER node
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:3000/health || exit 1

CMD ["node", "analytics-proxy.js"]
