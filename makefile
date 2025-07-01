# 设置目标操作系统和架构
GOOS := linux
GOARCH := amd64

# 编译器环境变量
export GOOS
export GOARCH

# 可执行文件名称与源路径映射
BINARIES := \
    craq-coordinator \
    craq-node \
    craq-client \
    craq-benchtest \
    craq-generate

SOURCES := \
    ./cmd/coordinator/coordinator.go \
    ./cmd/node/node.go \
    ./cmd/client/client.go \
    ./cmd/bench_test/benchtest.go \
    ./cmd/generate/generate.go

# 默认目标：编译所有程序
all: $(BINARIES)

craq-coordinator: ./cmd/coordinator/coordinator.go
	go build -o $@ $<

craq-node: ./cmd/node/node.go
	go build -o $@ $<

craq-client: ./cmd/client/client.go
	go build -o $@ $<

craq-benchtest: ./cmd/bench_test/benchtest.go
	go build -o $@ $<

craq-generate: ./cmd/generate/generate.go
	go build -o $@ $<

# 清理生成的可执行文件
clean:
	rm -f $(BINARIES)
