# Use an official Node.js runtime as the base image
FROM node:20-alpine AS base

# Check https://github.com/nodejs/docker-node/tree/b4117f9333da4138b03a546ec926ef50a31506c3#nodealpine
RUN apk add --no-cache libc6-compat

# Set the working directory in the container
WORKDIR /app

# Copy package.json and package-lock.json
COPY package*.json ./

# Install dependencies
RUN npm ci

# Copy the rest of the application code (but .env and node_modules will be ignored)
COPY . .

# Build the Next.js application
RUN npm run build

# Production image, copy all the files and run next
FROM node:20-alpine AS runner

WORKDIR /app

# Copy only necessary files from build stage
COPY --from=base /app/node_modules ./node_modules
COPY --from=base /app/package.json ./package.json
# Copy the entire dist directory if it exists
COPY --from=base /app/dist ./dist
# Copy the entire src directory if needed for runtime
COPY --from=base /app/src ./src

# Create non-root user
RUN adduser -D -u 1001 app
USER app

EXPOSE 3003

CMD ["npm", "run", "start"]