# =============================================================================
# Laravel App Dockerfile
# =============================================================================
#
# DESIGN DECISION: Build vs Volume
# The Dockerfile installs all system dependencies and PHP extensions.
# The actual Laravel application code is NOT baked in here —
# docker-compose.yml mounts ./laravel:/var/www/html at runtime.
#
# This means:
#   - Code changes on your host are instantly reflected in the container
#   - No rebuild needed when you edit PHP files
#   - You DO need to rebuild when system deps or PHP extensions change
#
# =============================================================================

FROM php:8.3-cli-alpine

# Install system dependencies
# libpq-dev:      PostgreSQL client libraries (needed by pdo_pgsql)
# librdkafka-dev: Kafka C client library (needed by php-rdkafka)
# oniguruma-dev:  Multibyte string support (needed by mbstring)
# bash + git:     Shell tools for development convenience
RUN apk add --no-cache \
    $PHPIZE_DEPS \
    bash \
    git \
    curl \
    libpq-dev \
    librdkafka-dev \
    oniguruma-dev

# Install PHP extensions
# pdo_pgsql: PostgreSQL database driver
# pgsql:     PostgreSQL native functions
# mbstring:  Multibyte string handling (required by Laravel)
# pcntl:     Process control (required for signal handling in consumers)
RUN docker-php-ext-install \
    pdo \
    pdo_pgsql \
    pgsql \
    mbstring \
    pcntl

# Install rdkafka PHP extension (Kafka client library)
# This wraps librdkafka (the C library) with a PHP API.
# rdkafka is the standard Kafka client for PHP.
RUN pecl install rdkafka && docker-php-ext-enable rdkafka

# Install Composer globally
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /var/www/html

# Install Composer dependencies using the mounted volume's composer.json.
# We use a multi-step approach for Docker layer caching:
#   Step 1: copy only composer.json + composer.lock → install deps
#           (this layer is cached unless lock file changes)
#   Step 2: copy full app → dump autoload
#
# However, since we're using a volume mount at runtime, we pre-install
# deps here so they exist inside the container on first run.
COPY laravel/composer.json laravel/composer.lock ./
RUN composer install --no-scripts --no-autoloader --prefer-dist --no-interaction

COPY laravel/ .
RUN composer dump-autoload --optimize

# Fix storage directory permissions
RUN chmod -R 775 storage bootstrap/cache

EXPOSE 8000

# php artisan serve: Laravel's built-in development HTTP server
# Listens on all interfaces (0.0.0.0) so Docker port mapping works.
CMD ["php", "artisan", "serve", "--host=0.0.0.0", "--port=8000"]
