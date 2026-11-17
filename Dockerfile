FROM node:22-alpine

WORKDIR /app

# Install dependency lebih dulu supaya layer ini bisa di-cache Docker
COPY package.json package-lock.json* ./
RUN npm install --omit=dev

COPY src ./src
COPY scripts ./scripts

# Default: tidak melakukan apa-apa. Perintah sebenarnya diatur
# per-service di docker-compose.yml
CMD ["node", "--version"]
