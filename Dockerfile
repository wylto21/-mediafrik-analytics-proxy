FROM node:22-alpine

WORKDIR /app
COPY analytics-proxy.js ./

ENV BIND=0.0.0.0 \
    PORT=3000 \
    NODE_ENV=production

USER node
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:3000/health || exit 1

CMD ["node", "analytics-proxy.js"]
