build:
	cd data-embed && cargo build --release
	mkdir -p data-kernel/bin
	cp data-embed/target/release/data-run data-kernel/bin/

.PHONY: build
