.PHONY: build test vet smoke clean install uninstall

BINARY := lzt-broker-stall-reaper
PKG := ./cmd/lzt-broker-stall-reaper/
INSTALL_DIR := /usr/local/bin
SYSTEMD_DIR := /etc/systemd/system

# Build the Go binary stripped
build:
	go build -trimpath -ldflags="-s -w" -o $(BINARY) $(PKG)

# Run go vet
vet:
	go vet $(PKG)

# Run unit tests (placeholder; main package has no tests yet)
test:
	go test -race -v $(PKG)

# One-shot smoke check: verify CLI + basic flow without root
smoke:
	./$(BINARY) -h > /dev/null
	./$(BINARY) -once -state-dir=/tmp/lzt-smoke -prom-dir=/tmp/lzt-smoke || true
	@echo "smoke OK (errors expected without root: ss --kill will fail)"

# Clean build artifacts
clean:
	rm -f $(BINARY)

# Install binary + systemd units
install: build
	install -m 0755 $(BINARY) $(INSTALL_DIR)/
	install -m 0644 systemd/$(BINARY).service $(SYSTEMD_DIR)/
	install -m 0644 systemd/$(BINARY).timer $(SYSTEMD_DIR)/
	systemctl daemon-reload
	systemctl enable --now $(BINARY).timer

# Remove installed artifacts
uninstall:
	systemctl disable --now $(BINARY).timer || true
	rm -f $(SYSTEMD_DIR)/$(BINARY).service
	rm -f $(SYSTEMD_DIR)/$(BINARY).timer
	rm -f $(INSTALL_DIR)/$(BINARY)
	systemctl daemon-reload
