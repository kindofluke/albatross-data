# Copyright 2023 DataRobot, Inc. and its affiliates.
# All rights reserved.
# DataRobot, Inc. Confidential.
# This is unpublished proprietary source code of DataRobot, Inc.
# and its affiliates.
# The copyright notice above does not evidence any actual or intended
# publication of such source code.


###################   !NOTA BENE!   #######################
#   All the files, parameters and packages are necessary  #
#   for the proper functioning of Notebooks.              #
#   If needed, you can include any system package         #
#   that will be installed through yum or            #
#   add a required package to the requirements.txt file.  #
#   Please note that removing predefined packages         #
#   may result in issues with Notebooks functionality.    #
###########################################################

ARG WORKDIR=/etc/system/kernel
ARG AGENTDIR=/etc/system/kernel/agent
ARG VENV_PATH=${WORKDIR}/.venv

ARG UNAME=notebooks
ARG UID=10101
ARG GID=10101


FROM golang:1.22.8 as ghcli_builder

WORKDIR /workspace

RUN git clone https://github.com/cli/cli.git gh-cli

WORKDIR /workspace/gh-cli

RUN git checkout v2.58.0  \
    && CGO_ENABLED=0 make install prefix=/workspace/gh

FROM registry.access.redhat.com/ubi8/ubi-minimal:8.10 AS pulumi_builder
RUN microdnf update \
  && microdnf install -y tar gzip \
  && microdnf clean all

RUN curl -o install_pulumi.sh https://get.pulumi.com && chmod +x install_pulumi.sh
RUN ./install_pulumi.sh --version latest --install-root ./pulumi --no-edit-path

FROM nvidia/cuda:13.1.1-cudnn-devel-ubuntu22.04 AS base
# some globally required dependencies

ARG UNAME
ARG UID
ARG GID
ARG WORKDIR
ARG AGENTDIR
ARG VENV_PATH


# Set the SHELL option -o pipefail before RUN with a pipe in it.
# Rationale: https://github.com/hadolint/hadolint/wiki/DL4006
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# Add any package that will be installed on system level here:
RUN apt-get update -y \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  software-properties-common build-essential \
  python3 python3-dev python3-venv python3-pip python3-distutils \
  gcc g++ gfortran libffi-dev libssl-dev libopenblas-dev liblapack-dev \
  git openssh-server tar gzip unzip zip wget curl ca-certificates \
  openjdk-11-jre-headless vim nano procps tzdata poppler-utils \
  pkg-config cmake ninja-build \
  protobuf-compiler libprotobuf-dev \
  clang mold \
  rustc cargo \
  libtcmalloc-minimal4 \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
  PYTHONDONTWRITEBYTECODE=1 \
  VENV_PATH=${VENV_PATH} \
  PIP_NO_CACHE_DIR=1 \
  NOTEBOOKS_KERNEL="python"

ENV PATH="$VENV_PATH/bin:$PATH" \
  PYTHONPATH="/home/notebooks/.ipython/extensions:/home/notebooks/storage"

RUN python3 -m venv ${VENV_PATH} && ${VENV_PATH}/bin/pip install -U pip setuptools
WORKDIR ${WORKDIR}

COPY ./agent/agent.py ./agent/cgroup_watchers.py ${AGENTDIR}/
COPY ./extensions /etc/ipython/extensions
COPY ./jupyter_kernel_gateway_config.py ./start_server.sh ${WORKDIR}/
COPY ./ipython_config.py /etc/ipython/

# Adding SSHD requirements
COPY ./sshd_config /etc/ssh/
RUN cp -a /etc/ssh /etc/ssh.cache && rm -rf /var/cache/apk/*
RUN mkdir /etc/authorized_keys

# Custom user to run the image from

RUN groupadd -g $GID -o $UNAME && \
  useradd -l -m -u $UID -g $GID -o -s /bin/bash $UNAME

# Shell customizations
COPY ./setup-shell.sh /etc/profile.d/setup-shell.sh

# additional setup scripts
COPY ./setup-ssh.sh ./common-user-limits.sh ./setup-venv.sh ${WORKDIR}/

# Adding SSHD requirements
RUN chown -R $UNAME:$UNAME ${WORKDIR} ${VENV_PATH} /home/notebooks /etc/ssh /etc/authorized_keys \
  # sshd prep
  && touch /etc/profile.d/notebooks-load-env.sh \
  && chown -R $UNAME:$UNAME /etc/profile.d/notebooks-load-env.sh \
  # Limit max processes
  && touch /etc/profile.d/bash-profile-load.sh \
  && chown -R $UNAME:$UNAME /etc/profile.d/bash-profile-load.sh


USER $UNAME

# Jupyter Gateway port
EXPOSE 8888
# sshd port
EXPOSE 22

FROM base AS minimal
# this stage has only bare minimal of dependencies installed to optimize build time for the local development

ARG WORKDIR
ARG VENV_PATH

COPY ./dr_requirements.txt ./agent/requirements-agent.txt ${WORKDIR}/
RUN pip install --no-cache-dir -r ${WORKDIR}/dr_requirements.txt \
  && pip install --no-cache-dir -r ${WORKDIR}/requirements-agent.txt \
  && rm ${WORKDIR}/dr_requirements.txt \
  && rm ${WORKDIR}/requirements-agent.txt \
  && rm ${VENV_PATH}/share/jupyter/kernels/python3/kernel.json \
  && chmod a+x ${WORKDIR}/start_server.sh

# Monitoring agent port
EXPOSE 8889

FROM minimal AS builder
# this stage includes all data science dependencies we want to have in the kernel runtime out of the box

ARG WORKDIR
ARG VENV_PATH
ARG PYTHON_VERSION

COPY ./kernel.json ${VENV_PATH}/share/jupyter/kernels/python3/
COPY ./requirements.txt ${WORKDIR}/
COPY ./requirements-gpu.txt ${WORKDIR}/
COPY ./data_kernel-*.whl ${WORKDIR}/
RUN pip install --use-deprecated=legacy-resolver --no-cache-dir -r ${WORKDIR}/requirements.txt \
  && pip install --no-cache-dir -r ${WORKDIR}/requirements-gpu.txt \
  && pip install --no-cache-dir ${WORKDIR}/data_kernel-*.whl \
  && rm ${WORKDIR}/requirements.txt && rm ${WORKDIR}/requirements-gpu.txt

FROM base AS kernel

# this stage is what actually going to be run as kernel image and it's clean from all build junks

ARG UNAME

ARG WORKDIR

ARG GIT_COMMIT

LABEL com.datarobot.repo-name="notebooks"
LABEL com.datarobot.repo-sha=$GIT_COMMIT

RUN chown -R $UNAME:$UNAME ${WORKDIR} /home/notebooks

COPY --from=builder --chown=$UNAME $WORKDIR $WORKDIR
COPY --from=ghcli_builder --chown=$UNAME /workspace/gh/bin/gh /usr/bin/gh
COPY --from=pulumi_builder --chown=$UNAME /pulumi/bin /usr/bin/

# This is required for custom models to work with this image
COPY ./start_server_drum.sh /opt/code/start_server.sh
ENV HOME=/opt CODE_DIR=/opt/code ADDRESS=0.0.0.0:8080
