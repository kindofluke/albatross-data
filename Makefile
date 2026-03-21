build:
	cd data-embed && cargo build --release
	mkdir -p data-kernel/bin
	cp data-embed/target/release/data-run data-kernel/bin/
	cp data-embed/target/release/libexecutor.dylib data-kernel/bin/
	cp data-embed/target/release/libexecutor.dylib data-kernel/src/data_kernel/
	cd data-kernel && uv build

.PHONY: build
