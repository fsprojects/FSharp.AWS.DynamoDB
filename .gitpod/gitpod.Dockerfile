FROM gitpod/workspace-base

############
### .Net ###
############
# Install .NET SDK (Current channel)
# Source: https://docs.microsoft.com/dotnet/core/install/linux-scripted-manual#scripted-install
LABEL dazzle/layer=dotnet
USER gitpod
ENV TRIGGER_REBUILD=1
ENV DOTNET_VERSION=5.0
ENV DOTNET_ROOT=/home/gitpod/dotnet
ENV PATH=$PATH:$DOTNET_ROOT
ENV PATH=$PATH:/home/gitpod/.dotnet/tools
ENV NUGET_PACKAGES=/workspace/nuget_cache
RUN mkdir -p $DOTNET_ROOT && curl -fsSL https://dot.net/v1/dotnet-install.sh | bash /dev/stdin --channel $DOTNET_VERSION --install-dir $DOTNET_ROOT
RUN dotnet tool install --global --version 4.6.0-alpha-009 fantomas-tool
RUN dotnet tool install --global paket
RUN dotnet tool install --global fake-cli

##############
### Docker ###
##############
LABEL dazzle/layer=docker
USER root
ENV TRIGGER_REBUILD=0
# https://docs.docker.com/engine/install/ubuntu/
RUN curl -o /var/lib/apt/dazzle-marks/docker.gpg -fsSL https://download.docker.com/linux/ubuntu/gpg \
    && apt-key add /var/lib/apt/dazzle-marks/docker.gpg \
    && add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    && install-packages docker-ce docker-ce-cli containerd.io

RUN curl -o /usr/bin/slirp4netns -fsSL https://github.com/rootless-containers/slirp4netns/releases/download/v1.1.12/slirp4netns-$(uname -m) \
    && chmod +x /usr/bin/slirp4netns

RUN curl -o /usr/local/bin/docker-compose -fsSL https://github.com/docker/compose/releases/download/1.29.2/docker-compose-Linux-x86_64 \
    && chmod +x /usr/local/bin/docker-compose
