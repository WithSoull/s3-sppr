package mocks

//go:generate go run github.com/gojuno/minimock/v3/cmd/minimock -i github.com/alesplll/opens3-rebac/services/sppr/internal/repository.EventRepository,github.com/alesplll/opens3-rebac/services/sppr/internal/service.Ingestor,github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db.TxManager,github.com/alesplll/opens3-rebac/services/sppr/internal/service/consumer/access.Observer -o . -s _minimock.go
