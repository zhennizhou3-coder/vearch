# 构建容器: docker build -t centos_dev_env .
# 运行容器：docker run -it --name centos_dev_container -p 9002:9002 centos_dev_en
# 使用 CentOS 8 作为基础镜像
FROM centos:8

# 设置环境变量，避免交互式提示
ENV DEBIAN_FRONTEND=noninteractive

# 安装必要的开发工具和依赖库
RUN yum -y update && yum install -y \
    gcc \
    gcc-c++ \
    make \
    cmake \
    git \
    wget \
    curl \
    vim \
    python3 \
    python3-pip \
    unzip \
    tar \
    libopenblas-dev \
    tbb-devel \
    epel-release \
    && yum clean all

# 安装 Go (1.22)
RUN wget https://golang.org/dl/go1.22.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.22.linux-amd64.tar.gz
RUN echo "export PATH=$PATH:/usr/local/go/bin" >> ~/.bashrc

# 安装 RocksDB（手动安装依赖）
RUN yum install -y \
    gcc-c++ \
    zlib-devel \
    bzip2-devel \
    snappy-devel \
    lz4-devel \
    && git clone --branch v6.6.4 https://github.com/facebook/rocksdb.git /root/rocksdb \
    && cd /root/rocksdb && make -j$(nproc) && make install

# 设置工作目录
WORKDIR /root/vearch

# 克隆 Vearch 源码并编译
RUN git clone https://github.com/vearch/vearch.git
WORKDIR /root/vearch
RUN make

# 安装 Python 库（如果需要）
RUN pip3 install -r requirements.txt

# 声明容器内部使用的端口
EXPOSE 9001

# 默认命令，启动容器时默认执行 bash
CMD ["/bin/bash"]
