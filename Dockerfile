# Use official Node.js LTS image on Alpine (small + fast)
FROM node:20-alpine

# Set working directory inside the container
WORKDIR /app

# Copy package files first (Docker layer caching — only re-installs if these change)
COPY package*.json ./

# Install production dependencies only
RUN npm install --omit=dev

# Copy the rest of your source code
COPY . .

# Default port your server listens on
EXPOSE 5000

# Default command — overridden per-service in docker-compose.yaml
CMD ["node", "server.js"]