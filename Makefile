DATA_KERNEL_DIR := data-kernel
DATA_KERNEL_DOCKER_DIR := $(DATA_KERNEL_DIR)/docker
DATA_KERNEL_DIST_DIR := $(DATA_KERNEL_DIR)/dist

build:
	cd data-embed && cargo build --release
	mkdir -p $(DATA_KERNEL_DIR)/bin
	cp data-embed/target/release/data-run $(DATA_KERNEL_DIR)/bin/
	cp data-embed/target/release/libexecutor.dylib $(DATA_KERNEL_DIR)/bin/
	cp data-embed/target/release/libexecutor.dylib $(DATA_KERNEL_DIR)/src/data_kernel/
	cd $(DATA_KERNEL_DIR) && uv build

.PHONY: build

# Build data_kernel wheel and copy into docker directory for Docker builds
.PHONY: data-kernel-wheel
data-kernel-wheel:
	cd $(DATA_KERNEL_DIR) && uv build
	cd $(DATA_KERNEL_DIR) && WHEEL=$$(ls dist/data_kernel-*.whl | sort | tail -n 1) && cp $$WHEEL docker/

# Create a docker build context zip containing everything in data-kernel/docker at the archive root
.PHONY: docker-context-zip
docker-context-zip: data-kernel-wheel
	cd $(DATA_KERNEL_DOCKER_DIR) && zip -r docker-context.zip .
