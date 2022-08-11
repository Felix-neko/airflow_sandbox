FROM ubuntu:22.04

RUN apt update && \
    apt install software-properties-common -y

RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    apt update && \
    apt install python3.7 -y

RUN apt install curl -y && \
    curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add && \
    apt-add-repository "deb http://apt.kubernetes.io/ kubernetes-xenial main" && \
    apt install kubeadm kubelet kubectl kubernetes-cni -y

RUN apt install python3-pip python3-distutils python3-apt python3.7-distutils -y
RUN python3.7 -m pip install kubernetes