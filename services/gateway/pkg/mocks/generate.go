package mocks

//go:generate go run github.com/gojuno/minimock/v3/cmd/minimock@v3.4.7 -g -i github.com/alesplll/opens3-rebac/services/gateway/internal/client.MetadataClient -o . -s _minimock.go
//go:generate go run github.com/gojuno/minimock/v3/cmd/minimock@v3.4.7 -g -i github.com/alesplll/opens3-rebac/services/gateway/internal/client.StorageClient -o . -s _minimock.go
//go:generate go run github.com/gojuno/minimock/v3/cmd/minimock@v3.4.7 -g -i github.com/alesplll/opens3-rebac/services/gateway/internal/service.ObjectService -o . -s _minimock.go

//go:generate go run github.com/gojuno/minimock/v3/cmd/minimock@v3.4.7 -g -i github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi.AccessRecorder -o . -s _minimock.go
//go:generate go run github.com/gojuno/minimock/v3/cmd/minimock@v3.4.7 -g -i github.com/alesplll/opens3-rebac/shared/pkg/go-kit/kafka.Producer -o . -s _minimock.go
