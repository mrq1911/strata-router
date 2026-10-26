FROM node:24-alpine
WORKDIR /app
COPY router.mjs package.json ./
RUN mkdir /data && chown node:node /data
USER node
EXPOSE 8000
CMD ["node", "router.mjs"]
