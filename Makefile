VERSION ?= v0.1.0
REG ?= quay.io/bharathappali/kube-gen-x
PLATFORMS ?= linux/amd64,linux/arm64

.PHONY: build test images push deploy e2e undeploy ui

build: ui
	cd controller && CGO_ENABLED=0 go build -o ../bin/controller ./cmd/controller
	cd controller && CGO_ENABLED=0 go build -o ../bin/tools ./cmd/tools
	cd worker && cargo build --release

ui:
	cd ui && npm ci && npm run build

test:
	cd controller && go test ./...
	cd worker && cargo test

images:
	docker buildx build --platform $(PLATFORMS) -f docker/controller.Dockerfile -t $(REG):controller-$(VERSION) -t $(REG):controller-latest --load=false .
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
