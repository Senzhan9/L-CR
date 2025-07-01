# 基础镜像
FROM alpine:3.21.3

# 安装 tc (iproute2) + bash
RUN apk update && apk add --no-cache \
    bash          \ 
    util-linux    \ 
    procps        \
    iproute2      \
    iputils       \ 
    curl          \ 
    git           \ 
    vim           \ 
    ca-certificates \
 && update-ca-certificates

WORKDIR /opt/craq

# 拷入本地交叉编译好的可执行
COPY craq-coordinator        /opt/craq/craq-coordinator
COPY craq-node               /opt/craq/craq-node
COPY craq-client             /opt/craq/craq-client
COPY craq-benchtest          /opt/craq/craq-bench-test
COPY craq-generate           /opt/craq/craq-generate

# 拷入启动脚本
COPY setup_tc.sh            /opt/craq/setup_tc.sh

# 授可执行权限
RUN chmod +x /opt/craq/*

# 容器入口：先跑 setup_tc.sh，再执行下面的 CMD
ENTRYPOINT ["/opt/craq/setup_tc.sh"]

# 默认启动 Coordinator，注意这里用的是绝对路径
CMD ["/opt/craq/craq-coordinator", "-a", ":1234"]
