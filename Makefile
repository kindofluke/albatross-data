DATA_KERNEL_DIR := data-kernel
DATA_KERNEL_DOCKER_DIR := $(DATA_KERNEL_DIR)/docker
DATA_KERNEL_DIST_DIR := $(DATA_KERNEL_DIR)/dist

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build                - Build Rust library and Python wheel"
	@echo "  data-kernel-wheel    - Build Python wheel and copy to docker/"
	@echo "  docker-source-prep   - Copy source files to docker/source/ for Docker build"
	@echo "  docker-build         - Build Docker image tagged as data_kernel_0_1"
	@echo "  docker-context-zip   - Create docker build context zip"

.DEFAULT_GOAL := help

build:
	cd data-embed && cargo build --release
	mkdir -p $(DATA_KERNEL_DIR)/bin
	cp data-embed/target/release/data-run $(DATA_KERNEL_DIR)/bin/
	cp data-embed/target/release/libexecutor.so $(DATA_KERNEL_DIR)/bin/
	cp data-embed/target/release/libexecutor.so $(DATA_KERNEL_DIR)/src/data_kernel/
	cd $(DATA_KERNEL_DIR) && uv build

.PHONY: build

# Build data_kernel wheel and copy into docker directory for Docker builds
.PHONY: data-kernel-wheel
data-kernel-wheel:
	cd $(DATA_KERNEL_DIR) && uv build
	cd $(DATA_KERNEL_DIR) && WHEEL=$$(ls dist/data_kernel-*.whl | sort | tail -n 1) && cp $$WHEEL docker/

# Copy source files to docker/source/ for building inside Docker
.PHONY: docker-source-prep
docker-source-prep:
	rm -rf $(DATA_KERNEL_DOCKER_DIR)/source
	mkdir -p $(DATA_KERNEL_DOCKER_DIR)/source
	rsync -a --exclude='target' data-embed/ $(DATA_KERNEL_DOCKER_DIR)/source/data-embed/
	rsync -a --exclude='dist' --exclude='build' --exclude='bin' --exclude='docker' \
		--exclude='*.egg-info' --exclude='__pycache__' --exclude='*.so' \
		data-kernel/ $(DATA_KERNEL_DOCKER_DIR)/source/data-kernel/

# Build Docker image
.PHONY: docker-build
docker-build: docker-source-prep
	cd $(DATA_KERNEL_DOCKER_DIR) && docker build -t data_kernel_0_1 .

# Create a docker build context zip containing everything in data-kernel/docker at the archive root
.PHONY: docker-context-zip
docker-context-zip: docker-source-prep
	cd $(DATA_KERNEL_DOCKER_DIR) && zip -r docker-context.zip .
