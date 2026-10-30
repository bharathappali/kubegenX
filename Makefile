VERSION ?= v0.1.1
REG ?= quay.io/bharathappali/kube-gen-x
PLATFORMS ?= linux/amd64,linux/arm64

.PHONY: build test images push deploy e2e undeploy ui

build: ui
	rm -f bin/controller bin/tools worker/target/release/worker
	cd controller && CGO_ENABLED=0 go build -o ../bin/controller ./cmd/controller
	cd controller && CGO_ENABLED=0 go build -o ../bin/tools ./cmd/tools
	cd worker && cargo build --release

ui:
	cd ui && npm ci && npm run build

test:
	cd controller && go test ./...
	cd worker && cargo test

images:
	rm -f bin/controller bin/controller-* bin/converter bin/converter-* bin/tools bin/tools-*
	mkdir -p bin
	cd controller && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../bin/controller-amd64 ./cmd/controller
	cd controller && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../bin/controller-arm64 ./cmd/controller
	cd controller && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../bin/converter-amd64 ./cmd/converter
	cd controller && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../bin/converter-arm64 ./cmd/converter
	cd controller && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../bin/tools-amd64 ./cmd/tools
	cd controller && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o ../bin/tools-arm64 ./cmd/tools
	docker buildx build --platform $(PLATFORMS) -f docker/controller.Dockerfile -t $(REG):controller-$(VERSION) -t $(REG):controller-latest --load=false .
	docker buildx build --platform $(PLATFORMS) -f docker/converter.Dockerfile -t $(REG):converter-$(VERSION) -t $(REG):converter-latest --load=false .
	docker buildx build --platform $(PLATFORMS) -f docker/worker.Dockerfile -t $(REG):worker-$(VERSION) -t $(REG):worker-latest --load=false .
	docker buildx build --platform $(PLATFORMS) -f docker/tools.Dockerfile -t $(REG):tools-$(VERSION) -t $(REG):tools-latest --load=false .

push:
	hack/images.sh

deploy:
	hack/deploy.sh

e2e:
	hack/e2e.sh

undeploy:
	hack/undeploy.sh
