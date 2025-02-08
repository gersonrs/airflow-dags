FROM quay.io/astronomer/astro-runtime:12.6.0

# Switch to root for setup
USER root

# Install system packages if listed in packages.txt
COPY packages.txt .
RUN /usr/local/bin/install-system-packages

# Install Python dependencies
COPY requirements.txt .
RUN /usr/local/bin/install-python-dependencies

# Switch back to astro user
USER astro

# Copy project into image
COPY --chown=astro:0 . .
