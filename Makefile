LOCAL_BIN=$(CURDIR)/bin
GO_PROTO_OUT=$(CURDIR)/shared/pkg/go
PY_PROTO_OUT=$(CURDIR)/shared/pkg/py
PYTHON?=python3
PY_PROTO_DEPS=$(LOCAL_BIN)/python

# ── Dependencies ───────────────────────────────────────────────────────────────

install-deps:
	mkdir -p $(LOCAL_BIN)
	GOBIN=$(LOCAL_BIN) go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28.1
	GOBIN=$(LOCAL_BIN) go install -mod=mod google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
	$(PYTHON) -m pip install --upgrade --target "$(PY_PROTO_DEPS)" grpcio-tools==1.74.0

# ── Proto generation ───────────────────────────────────────────────────────────

generate: generate-go generate-py

generate-all: generate
generate-all-go: generate-go
generate-all-py: generate-py

generate-go: generate-user-go generate-auth-go generate-storage-go generate-metadata-go generate-authz-go generate-quota-go generate-events-go

generate-py: generate-user-py generate-auth-py generate-storage-py generate-metadata-py generate-authz-py generate-quota-py generate-events-py

generate-user-go:
	mkdir -p $(GO_PROTO_OUT)/user/v1
	protoc \
		--proto_path shared/api/user/v1 \
		--go_out=$(GO_PROTO_OUT)/user/v1 --go_opt=paths=source_relative \
		--plugin=protoc-gen-go=$(LOCAL_BIN)/protoc-gen-go \
		--go-grpc_out=$(GO_PROTO_OUT)/user/v1 --go-grpc_opt=paths=source_relative \
		--plugin=protoc-gen-go-grpc=$(LOCAL_BIN)/protoc-gen-go-grpc \
		shared/api/user/v1/user.proto

generate-auth-go:
	mkdir -p $(GO_PROTO_OUT)/auth/v1
	protoc \
		--proto_path shared/api/auth/v1 \
		--go_out=$(GO_PROTO_OUT)/auth/v1 --go_opt=paths=source_relative \
		--plugin=protoc-gen-go=$(LOCAL_BIN)/protoc-gen-go \
		--go-grpc_out=$(GO_PROTO_OUT)/auth/v1 --go-grpc_opt=paths=source_relative \
		--plugin=protoc-gen-go-grpc=$(LOCAL_BIN)/protoc-gen-go-grpc \
		shared/api/auth/v1/auth.proto

generate-storage-go:
	mkdir -p $(GO_PROTO_OUT)/storage/v1
	protoc \
		--proto_path shared/api/storage/v1 \
		--go_out=$(GO_PROTO_OUT)/storage/v1 --go_opt=paths=source_relative \
		--plugin=protoc-gen-go=$(LOCAL_BIN)/protoc-gen-go \
		--go-grpc_out=$(GO_PROTO_OUT)/storage/v1 --go-grpc_opt=paths=source_relative \
		--plugin=protoc-gen-go-grpc=$(LOCAL_BIN)/protoc-gen-go-grpc \
		shared/api/storage/v1/storage.proto

generate-authz-go:
	mkdir -p $(GO_PROTO_OUT)/authz/v1
	protoc \
		--proto_path shared/api/authz/v1 \
		--go_out=$(GO_PROTO_OUT)/authz/v1 --go_opt=paths=source_relative \
		--plugin=protoc-gen-go=$(LOCAL_BIN)/protoc-gen-go \
		--go-grpc_out=$(GO_PROTO_OUT)/authz/v1 --go-grpc_opt=paths=source_relative \
		--plugin=protoc-gen-go-grpc=$(LOCAL_BIN)/protoc-gen-go-grpc \
		shared/api/authz/v1/authz.proto

generate-metadata-go:
	mkdir -p $(GO_PROTO_OUT)/metadata/v1
	protoc \
		--proto_path shared/api/metadata/v1 \
		--go_out=$(GO_PROTO_OUT)/metadata/v1 --go_opt=paths=source_relative \
		--plugin=protoc-gen-go=$(LOCAL_BIN)/protoc-gen-go \
		--go-grpc_out=$(GO_PROTO_OUT)/metadata/v1 --go-grpc_opt=paths=source_relative \
		--plugin=protoc-gen-go-grpc=$(LOCAL_BIN)/protoc-gen-go-grpc \
		shared/api/metadata/v1/metadata.proto shared/api/metadata/v1/placement.proto

generate-user-py:
	mkdir -p $(PY_PROTO_OUT)/user/v1
	PYTHONPATH="$(PY_PROTO_DEPS)" $(PYTHON) -m grpc_tools.protoc \
		--proto_path=shared/api/user/v1 \
		--python_out=$(PY_PROTO_OUT)/user/v1 \
		--grpc_python_out=$(PY_PROTO_OUT)/user/v1 \
		shared/api/user/v1/user.proto
	touch shared/__init__.py shared/pkg/__init__.py $(PY_PROTO_OUT)/__init__.py $(PY_PROTO_OUT)/user/__init__.py $(PY_PROTO_OUT)/user/v1/__init__.py
	perl -0pi -e 's/^import user_pb2 as user__pb2/from . import user_pb2 as user__pb2/m' $(PY_PROTO_OUT)/user/v1/user_pb2_grpc.py

generate-auth-py:
	mkdir -p $(PY_PROTO_OUT)/auth/v1
	PYTHONPATH="$(PY_PROTO_DEPS)" $(PYTHON) -m grpc_tools.protoc \
		--proto_path=shared/api/auth/v1 \
		--python_out=$(PY_PROTO_OUT)/auth/v1 \
		--grpc_python_out=$(PY_PROTO_OUT)/auth/v1 \
		shared/api/auth/v1/auth.proto
	touch shared/__init__.py shared/pkg/__init__.py $(PY_PROTO_OUT)/__init__.py $(PY_PROTO_OUT)/auth/__init__.py $(PY_PROTO_OUT)/auth/v1/__init__.py
	perl -0pi -e 's/^import auth_pb2 as auth__pb2/from . import auth_pb2 as auth__pb2/m' $(PY_PROTO_OUT)/auth/v1/auth_pb2_grpc.py

generate-storage-py:
	mkdir -p $(PY_PROTO_OUT)/storage/v1
	PYTHONPATH="$(PY_PROTO_DEPS)" $(PYTHON) -m grpc_tools.protoc \
		--proto_path=shared/api/storage/v1 \
		--python_out=$(PY_PROTO_OUT)/storage/v1 \
		--grpc_python_out=$(PY_PROTO_OUT)/storage/v1 \
		shared/api/storage/v1/storage.proto
	touch shared/__init__.py shared/pkg/__init__.py $(PY_PROTO_OUT)/__init__.py $(PY_PROTO_OUT)/storage/__init__.py $(PY_PROTO_OUT)/storage/v1/__init__.py
	perl -0pi -e 's/^import storage_pb2 as storage__pb2/from . import storage_pb2 as storage__pb2/m' $(PY_PROTO_OUT)/storage/v1/storage_pb2_grpc.py

generate-metadata-py:
	mkdir -p $(PY_PROTO_OUT)/metadata/v1
	PYTHONPATH="$(PY_PROTO_DEPS)" $(PYTHON) -m grpc_tools.protoc \
		--proto_path=shared/api/metadata/v1 \
		--python_out=$(PY_PROTO_OUT)/metadata/v1 \
		--grpc_python_out=$(PY_PROTO_OUT)/metadata/v1 \
		shared/api/metadata/v1/metadata.proto shared/api/metadata/v1/placement.proto
	touch shared/__init__.py shared/pkg/__init__.py $(PY_PROTO_OUT)/__init__.py $(PY_PROTO_OUT)/metadata/__init__.py $(PY_PROTO_OUT)/metadata/v1/__init__.py
	perl -0pi -e 's/^import metadata_pb2 as metadata__pb2/from . import metadata_pb2 as metadata__pb2/m' $(PY_PROTO_OUT)/metadata/v1/metadata_pb2_grpc.py
	perl -0pi -e 's/^import placement_pb2 as placement__pb2/from . import placement_pb2 as placement__pb2/m' $(PY_PROTO_OUT)/metadata/v1/placement_pb2_grpc.py

generate-authz-py:
	mkdir -p $(PY_PROTO_OUT)/authz/v1
	PYTHONPATH="$(PY_PROTO_DEPS)" $(PYTHON) -m grpc_tools.protoc \
		--proto_path=shared/api/authz/v1 \
		--python_out=$(PY_PROTO_OUT)/authz/v1 \
		--grpc_python_out=$(PY_PROTO_OUT)/authz/v1 \
		shared/api/authz/v1/authz.proto
	touch shared/__init__.py shared/pkg/__init__.py $(PY_PROTO_OUT)/__init__.py $(PY_PROTO_OUT)/authz/__init__.py $(PY_PROTO_OUT)/authz/v1/__init__.py
	perl -0pi -e 's/^import authz_pb2 as authz__pb2/from . import authz_pb2 as authz__pb2/m' $(PY_PROTO_OUT)/authz/v1/authz_pb2_grpc.py

generate-quota-go:
	mkdir -p $(GO_PROTO_OUT)/quota/v1
	protoc \
		--proto_path shared/api/quota/v1 \
		--go_out=$(GO_PROTO_OUT)/quota/v1 --go_opt=paths=source_relative \
		--plugin=protoc-gen-go=$(LOCAL_BIN)/protoc-gen-go \
		--go-grpc_out=$(GO_PROTO_OUT)/quota/v1 --go-grpc_opt=paths=source_relative \
		--plugin=protoc-gen-go-grpc=$(LOCAL_BIN)/protoc-gen-go-grpc \
		shared/api/quota/v1/quota.proto

generate-events-go:
	mkdir -p $(GO_PROTO_OUT)/events/v1
	protoc \
		--proto_path shared/api/events/v1 \
		--go_out=$(GO_PROTO_OUT)/events/v1 --go_opt=paths=source_relative \
		--plugin=protoc-gen-go=$(LOCAL_BIN)/protoc-gen-go \
		shared/api/events/v1/access.proto

# events contain messages only; protoc's built-in Python generator is sufficient.
generate-events-py:
	mkdir -p $(PY_PROTO_OUT)/events/v1
	protoc --proto_path=shared/api/events/v1 --python_out=$(PY_PROTO_OUT)/events/v1 shared/api/events/v1/access.proto
	touch $(PY_PROTO_OUT)/events/__init__.py $(PY_PROTO_OUT)/events/v1/__init__.py

generate-quota-py:
	mkdir -p $(PY_PROTO_OUT)/quota/v1
	PYTHONPATH="$(PY_PROTO_DEPS)" $(PYTHON) -m grpc_tools.protoc \
		--proto_path=shared/api/quota/v1 \
		--python_out=$(PY_PROTO_OUT)/quota/v1 \
		--grpc_python_out=$(PY_PROTO_OUT)/quota/v1 \
		shared/api/quota/v1/quota.proto
	touch shared/__init__.py shared/pkg/__init__.py $(PY_PROTO_OUT)/__init__.py $(PY_PROTO_OUT)/quota/__init__.py $(PY_PROTO_OUT)/quota/v1/__init__.py
	perl -0pi -e 's/^import quota_pb2 as quota__pb2/from . import quota_pb2 as quota__pb2/m' $(PY_PROTO_OUT)/quota/v1/quota_pb2_grpc.py

# ── Tests ──────────────────────────────────────────────────────────────────────

test-users:
	go test ./services/users/... -count=1

.PHONY: ml-sppr-venv ml-sppr-data ml-sppr-traces ml-sppr-train ml-sppr-train-all ml-sppr-report ml-sppr-notebook test-sppr-ml test-sppr test-sppr-integration up-sppr up-sppr-gateway up-sppr-migration up-sppr-planner smoke-sppr smoke-sppr-local require-gateway-access-producer demo-sppr-data dataset-sppr
SPPR_DATA_CONFIG ?= services/sppr/examples/data/small.json
SPPR_DATA_OUT ?= build/sppr-data
SPPR_SCENARIO ?= stable
SPPR_SEED ?= 0

demo-sppr-data:
	go run ./services/sppr/cmd/dataset -demo

dataset-sppr:
	go run ./services/sppr/cmd/dataset -config "$(SPPR_DATA_CONFIG)" -out "$(SPPR_DATA_OUT)" -scenario "$(SPPR_SCENARIO)" -seed "$(SPPR_SEED)"

SPPR_ML := services/sppr/ml
SPPR_ML_PY := $(SPPR_ML)/.venv/bin/python
SPPR_ML_DATA ?= build/ml-medium/shift-0
SPPR_ML_OUT ?= build/ml-models/shift-0

ml-sppr-venv:
	test -x $(SPPR_ML_PY) || python3 -m venv $(SPPR_ML)/.venv
	$(SPPR_ML_PY) -m pip install -q -r $(SPPR_ML)/requirements.txt

# Six synthetic scenarios, seed 0, 1000 objects: the stage 8 working set.
ml-sppr-data:
	for s in stable decay periodic burst shift uniform; do \
		test -d build/ml-medium/$$s-0 || go run ./services/sppr/cmd/dataset -config services/sppr/examples/data/medium.json -scenario $$s -seed 0 -out build/ml-medium/$$s-0 >/dev/null || exit 1; \
	done

# Real traces with parameters fixed in the script; inputs stay outside the repo.
SPPR_TRACE_DOWNLOADS ?= $(HOME)/home/98_Downloads
SPPR_META_TRACE ?= build/traces/meta/reag0c01_20230315_20230322_0.2000.csv.zst
ml-sppr-traces:
	mkdir -p build/bin
	go build -o build/bin/trace-convert ./services/sppr/cmd/trace-convert
	go build -o build/bin/dataset ./services/sppr/cmd/dataset
	$(SPPR_ML_PY) $(SPPR_ML)/scripts/trace_datasets.py --downloads "$(SPPR_TRACE_DOWNLOADS)" --meta "$(SPPR_META_TRACE)"

ml-sppr-train:
	PYTHONPATH=$(SPPR_ML) $(SPPR_ML_PY) -m sppr_ml train --data $(SPPR_ML_DATA) --out $(SPPR_ML_OUT)

ml-sppr-train-all: ml-sppr-data
	for s in stable decay periodic burst shift uniform; do \
		test -d build/ml-models/$$s-0 || $(MAKE) ml-sppr-train SPPR_ML_DATA=build/ml-medium/$$s-0 SPPR_ML_OUT=build/ml-models/$$s-0 || exit 1; \
	done

ml-sppr-report:
	PYTHONPATH=$(SPPR_ML) $(SPPR_ML_PY) -m sppr_ml report build/ml-models/*/report.json
	PYTHONPATH=$(SPPR_ML) $(SPPR_ML_PY) -m sppr_ml report --summary build/ml-models/*/report.json

ml-sppr-notebook:
	PYTHONPATH=$(CURDIR)/$(SPPR_ML) $(SPPR_ML_PY) $(SPPR_ML)/scripts/build_notebook.py
	cd $(SPPR_ML)/notebooks && PYTHONPATH=$(CURDIR)/$(SPPR_ML) ../.venv/bin/jupyter nbconvert --to notebook --execute --inplace stage8.ipynb

test-sppr-ml:
	cd $(SPPR_ML) && $(abspath $(SPPR_ML_PY)) -m pytest -q tests

test-sppr:
	go test -race ./services/sppr/... -count=1

test-sppr-integration:
	go test -race -tags=integration ./services/sppr/... -count=1

up-sppr:
	docker compose --profile sppr up --build -d sppr

require-gateway-access-producer:
	@test -f services/gateway/internal/service/producer/access/service.go || { echo 'Gateway producer is supplied by PR #1; merge both PRs for the combined smoke.' >&2; exit 1; }

up-sppr-gateway: require-gateway-access-producer
	ACCESS_EVENTS_ENABLED=true docker compose --profile services --profile sppr up --build -d gateway sppr metadata storage-hot storage-cold

up-sppr-migration:
	MIGRATION_ENABLED=true docker compose --profile services --profile sppr up --build -d gateway sppr metadata storage-hot storage-cold

up-sppr-planner:
	MIGRATION_ENABLED=true PLANNER_ENABLED=true docker compose --profile services --profile sppr up --build -d gateway sppr metadata storage-hot storage-cold auth authz users

smoke-sppr: require-gateway-access-producer
	SPPR_SMOKE_PG_DSN="$${SPPR_SMOKE_PG_DSN:-postgres://sppr_user:sppr_password@localhost:$${SPPR_DB_PORT:-5436}/sppr_db?sslmode=disable}" go run ./services/sppr/cmd/smoke

smoke-sppr-local: up-sppr-gateway
	$(MAKE) smoke-sppr

test-storage:
	go test ./services/storage/... -count=1

# Requires the running Gateway MVP stack; creates a unique smoke bucket.
.PHONY: test-gateway smoke-gateway smoke-gateway-local
smoke-gateway:
	go run ./services/gateway/cmd/smoke

smoke-gateway-local: up-gateway
	$(MAKE) smoke-gateway

test-gateway:
	cd services/gateway && go test ./... -count=1

test-users-service:
	go test ./services/users/internal/service/user/tests -count=1

test-metadata-integration:
	cd services/metadata && go test -p 1 -tags=integration ./internal/repository/bucket/tests ./internal/repository/object/tests ./internal/repository/placement/tests ./internal/service/placement/tests -count=1

test-metadata-integration-local: up-e2e
	cd services/metadata && go test -p 1 -tags=integration ./internal/repository/bucket/tests ./internal/repository/object/tests ./internal/repository/placement/tests ./internal/service/placement/tests -count=1

# ── Docker ─────────────────────────────────────────────────────────────────────

up-services:
	docker compose --profile services up -d

up-gateway:
	docker compose --profile services up --build -d gateway metadata storage-hot storage-cold

up-e2e:
	docker compose --env-file e2e/.env --profile e2e up -d --wait --force-recreate postgres-metadata-e2e

down-e2e:
	docker compose --env-file e2e/.env --profile e2e stop postgres-metadata-e2e
	docker compose --env-file e2e/.env --profile e2e rm -f postgres-metadata-e2e

up-observability:
	docker compose --profile observability up -d

up-all:
	docker compose --profile services --profile observability up -d

down:
	docker compose down

down-volumes:
	docker compose down -v

rebuild:
	docker compose --profile services build --no-cache
	docker compose --profile services up -d

.PHONY: smoke-migration smoke-migration-local
# Requires SPPR with MIGRATION_ENABLED=true and a dedicated Gateway stand.
smoke-migration:
	go run ./services/sppr/cmd/smoke-migration $(SMOKE_MIGRATION_ARGS)

smoke-migration-local:
	GATEWAY_MAX_READ_DURATION=2s MIGRATION_SOURCE_CLEANUP_DELAY=5s METADATA_DELETE_CLEANUP_DELAY=5s MIGRATION_CLEANUP_INTERVAL=250ms $(MAKE) up-sppr-migration
	$(MAKE) smoke-migration

.PHONY: seed-planner-admin smoke-planner smoke-planner-local
seed-planner-admin:
	go run ./services/sppr/cmd/seed-planner-admin

smoke-planner:
	go run ./services/sppr/cmd/smoke-planner $(SMOKE_PLANNER_ARGS)

smoke-planner-local:
	GATEWAY_MAX_READ_DURATION=2s MIGRATION_SOURCE_CLEANUP_DELAY=5s METADATA_DELETE_CLEANUP_DELAY=5s MIGRATION_CLEANUP_INTERVAL=250ms MIGRATION_MIN_INTERVAL=0s $(MAKE) up-sppr-planner
	$(MAKE) smoke-planner

# Logical-time experiments (stage 10a); each output directory is a new series.
CONFIG ?= services/sppr/experiments/main.json
EXPERIMENT_OUT ?= build/experiment-main
SIM_SMOKE_OUT ?= build/sim-smoke
.PHONY: sim-smoke experiment experiment-report
build/bin/experiment: FORCE
	mkdir -p build/bin
	go build -o $@ ./services/sppr/cmd/experiment
sim-smoke: build/bin/experiment
	GOMAXPROCS=2 build/bin/experiment -config services/sppr/experiments/smoke.json -out $(SIM_SMOKE_OUT) -python $(abspath $(SPPR_ML_PY))
experiment: build/bin/experiment
	GOMAXPROCS=2 build/bin/experiment -config $(CONFIG) -out $(EXPERIMENT_OUT) -python $(abspath $(SPPR_ML_PY)) $(EXPERIMENT_ARGS)
experiment-report:
	PYTHONPATH=$(SPPR_ML) $(SPPR_ML_PY) -m sppr_ml experiment $(EXPERIMENT_OUT)
.PHONY: FORCE
FORCE:

.PHONY: smoke-history smoke-history-local
# Disruptive: dedicated stand only; stops Kafka and kills/stops Gateway.
smoke-history:
	SPPR_SMOKE_PG_DSN="$${SPPR_SMOKE_PG_DSN:-postgres://sppr_user:sppr_password@localhost:$${SPPR_DB_PORT:-5436}/sppr_db?sslmode=disable}" go run ./services/sppr/cmd/smoke-history $(SMOKE_HISTORY_ARGS)

smoke-history-local:
	ACCESS_EVENTS_ENABLED=true ADMIN_AUTH_ENABLED=false ACCESS_HEARTBEAT_INTERVAL=1s INGEST_STREAM_LOST_AFTER=30s $(MAKE) up-sppr-planner
	$(MAKE) smoke-history

# Disposable real-service stands; always run from the repository root.
STAND_CONFIG ?= services/sppr/experiments/stand-shift.json
STAND_OUT ?= build/stand-shift
STAND_SMOKE_OUT ?= build/stand-smoke
OVERHEAD_CONFIG ?= services/sppr/experiments/overhead.json
OVERHEAD_OUT ?= build/overhead
.PHONY: stand-smoke stand-experiment overhead
stand-smoke: build/bin/experiment
	build/bin/experiment -config services/sppr/experiments/stand-smoke.json -out $(STAND_SMOKE_OUT) $(EXPERIMENT_ARGS)
stand-experiment: build/bin/experiment
	build/bin/experiment -config $(STAND_CONFIG) -out $(STAND_OUT) $(EXPERIMENT_ARGS)
overhead: build/bin/experiment
	build/bin/experiment -config $(OVERHEAD_CONFIG) -out $(OVERHEAD_OUT) $(EXPERIMENT_ARGS)
