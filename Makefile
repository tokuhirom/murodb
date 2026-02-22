TLA_TOOLS_DIR ?= .tools
TLA2TOOLS_JAR ?= $(TLA_TOOLS_DIR)/tla2tools.jar
TLA2TOOLS_URL ?= https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar

.PHONY: tlc-tools tlc tlc-large test bench bench-snippet

tlc-tools: $(TLA2TOOLS_JAR)

$(TLA2TOOLS_JAR):
	@mkdir -p "$(TLA_TOOLS_DIR)"
	@if command -v curl >/dev/null 2>&1; then \
		echo "Downloading TLA+ tools with curl..."; \
		curl -fL "$(TLA2TOOLS_URL)" -o "$(TLA2TOOLS_JAR)"; \
	elif command -v wget >/dev/null 2>&1; then \
		echo "Downloading TLA+ tools with wget..."; \
		wget -O "$(TLA2TOOLS_JAR)" "$(TLA2TOOLS_URL)"; \
	else \
		echo "Neither curl nor wget is available."; \
		exit 1; \
	fi
	@echo "Saved $(TLA2TOOLS_JAR)"

tlc: tlc-tools
	@TLA2TOOLS_JAR="$(abspath $(TLA2TOOLS_JAR))" ./specs/tla/run_tlc.sh

tlc-large: tlc-tools
	@TLA2TOOLS_JAR="$(abspath $(TLA2TOOLS_JAR))" ./specs/tla/run_tlc.sh ./specs/tla/CrashResilience.large.cfg

test:
	cargo test

bench:
	cargo run --release --bin murodb_bench

bench-snippet:
	cargo run --release --bin murodb_snippet_bench
