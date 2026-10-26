FROM node:24-alpine
WORKDIR /app
COPY router.mjs package.json ./
EXPOSE 8000
CMD ["node", "router.mjs"]
