FROM ubuntu:26.04


# Install system dependencies and Python tooling
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
        build-essential \
        curl \
        git \
        jq \
        locales \
        nodejs \
        openssh-client \
        python3 \
        python3-pip \
        python3-venv \
        ruby-full \
        sudo \
        vim \
        zip \
        unzip \
    && locale-gen en_US.UTF-8 \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Ensure ubuntu user can sudo without a password
RUN echo "ubuntu ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers.d/ubuntu \
    && chmod 0440 /etc/sudoers.d/ubuntu

# Install cfn-nag (Ruby gem)
RUN gem install cfn-nag --no-document

# Install AWS rain
RUN ARCH=$(dpkg --print-architecture) \
    && LATEST="$(curl -s https://api.github.com/repos/aws-cloudformation/rain/releases/latest | jq -r '.tag_name')" \
    && curl -L -o rain.zip "https://github.com/aws-cloudformation/rain/releases/download/${LATEST}/rain-${LATEST}_linux-${ARCH}.zip" \
    && unzip rain.zip \
    && mv "rain-${LATEST}_linux-${ARCH}/rain" /usr/local/bin/ \
    && rm rain.zip

USER ubuntu

# Install Python tools inside a virtual environment
RUN python3 -m venv /home/ubuntu/pyenv \
    && /home/ubuntu/pyenv/bin/pip install --upgrade pip setuptools wheel --no-cache-dir \
    && /home/ubuntu/pyenv/bin/pip install --no-cache-dir \
        cfn-lint \
        boto3 \
        awscli \
        pre-commit \
        pre-commit-hooks \
        black \
        pydot \
        pyyaml \
        uv

# Make the virtual environment the default Python and set locale
ENV PATH="/home/ubuntu/pyenv/bin:$PATH" \
    LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8 \
    LANGUAGE=en_US:en

# pre-install MCP servers so they don't need to be downloaded on first run
RUN uv tool install mcp-proxy-for-aws \
  && uv tool install awslabs.cloudwatch-mcp-server \
  && uv tool install awslabs.aws-iac-mcp-server

ENV PATH="/home/ubuntu/.local/bin:$PATH"

# Set up working directory
WORKDIR /home/ubuntu/
