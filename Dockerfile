# --- Stage 1: Build the Vite Frontend ---
FROM node:18 AS build-stage
WORKDIR /app
COPY . .
RUN rm -rf node_modules package-lock.json && npm install
RUN npm run build

# --- Stage 2: Setup the Python Backend ---
FROM python:3.10-slim
WORKDIR /app

# Copy python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY . .

# Copy built frontend from Stage 1 into the "dist" directory
# which app.py expects to serve
COPY --from=build-stage /app/dist /app/dist

# Expose the port (Render provides $PORT dynamically, but defaults to 3003)
EXPOSE 3003

# Use gunicorn to run the Flask app in production
# Run it with a wrapper that listens to $PORT environment variable
CMD ["sh", "-c", "gunicorn -w 2 -b 0.0.0.0:${PORT:-3003} app:app"]
