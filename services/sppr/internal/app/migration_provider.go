package app

import (
	"context"
	"fmt"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	metadataClient "github.com/alesplll/opens3-rebac/services/sppr/internal/client/metadata"
	storageClient "github.com/alesplll/opens3-rebac/services/sppr/internal/client/storage"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/observability"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	actionRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/action"
	pinRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/pin"
	settingsRepository "github.com/alesplll/opens3-rebac/services/sppr/internal/repository/settings"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	cleanupService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/cleanup"
	executorService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/executor"
	migrationService "github.com/alesplll/opens3-rebac/services/sppr/internal/service/migration"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/metric"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/tracing"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Workers look for new actions this often when idle or after an error.
const migrationPollInterval = time.Second

func (s *serviceProvider) ActionRepository(ctx context.Context) (repository.ActionRepository, error) {
	if s.actionRepository == nil {
		pgClient, err := s.PGClient(ctx)
		if err != nil {
			return nil, err
		}
		s.actionRepository = actionRepository.NewRepository(pgClient)
	}
	return s.actionRepository, nil
}

// SettingsRepository also stores the env defaults on the first start.
func (s *serviceProvider) SettingsRepository(ctx context.Context) (repository.SettingsRepository, error) {
	if s.settingsRepository == nil {
		pgClient, err := s.PGClient(ctx)
		if err != nil {
			return nil, err
		}
		repo := settingsRepository.NewRepository(pgClient)
		cfg := s.cfg.Migration
		if err := repo.EnsureDefaults(ctx, model.MigrationSettings{
			BudgetBytes:  cfg.BudgetBytes(),
			BudgetWindow: cfg.BudgetWindow(),
			MinInterval:  cfg.MinInterval(),
		}); err != nil {
			return nil, fmt.Errorf("store default migration settings: %w", err)
		}
		s.settingsRepository = repo
	}
	return s.settingsRepository, nil
}

func (s *serviceProvider) PinRepository(ctx context.Context) (repository.PinRepository, error) {
	if s.pinRepository == nil {
		pgClient, err := s.PGClient(ctx)
		if err != nil {
			return nil, err
		}
		s.pinRepository = pinRepository.NewRepository(pgClient)
	}
	return s.pinRepository, nil
}

func (s *serviceProvider) MigrationService(ctx context.Context) (service.MigrationService, error) {
	if s.migrationService == nil {
		actions, err := s.ActionRepository(ctx)
		if err != nil {
			return nil, err
		}
		settings, err := s.SettingsRepository(ctx)
		if err != nil {
			return nil, err
		}
		pins, err := s.PinRepository(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := s.TxManager(ctx)
		if err != nil {
			return nil, err
		}
		metrics, err := observability.NewMigrationMetrics(metric.Meter())
		if err != nil {
			return nil, err
		}
		s.migrationMetrics = metrics
		s.migrationService = migrationService.NewService(actions, settings, pins, tx, metrics)
		if err := metrics.RegisterBudget(s.migrationService.GetBudget); err != nil {
			return nil, err
		}
	}
	return s.migrationService, nil
}

func (s *serviceProvider) PlacementClient() (client.PlacementClient, error) {
	if s.placementClient == nil {
		conn, err := grpc.NewClient(
			s.cfg.Upstream.MetadataAddress(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithUnaryInterceptor(tracing.UnaryClientInterceptor("sppr.metadata")),
		)
		if err != nil {
			return nil, fmt.Errorf("create Metadata client: %w", err)
		}
		s.closer.AddNamed("Metadata gRPC connection", func(context.Context) error { return conn.Close() })
		s.placementClient = metadataClient.NewClient(metadatav1.NewPlacementServiceClient(conn), s.cfg.Upstream.MetadataTimeout())
	}
	return s.placementClient, nil
}

// StorageClients reads and writes Storage directly, so technical copies do not
// pass through Gateway and do not show up as object-read demand.
func (s *serviceProvider) StorageClients() (map[model.StorageClass]client.StorageClient, error) {
	if s.storageClients == nil {
		addresses := map[model.StorageClass]string{
			model.StorageClassHot:  s.cfg.Upstream.StorageHotAddress(),
			model.StorageClassCold: s.cfg.Upstream.StorageColdAddress(),
		}
		clients := make(map[model.StorageClass]client.StorageClient, len(addresses))
		for class, address := range addresses {
			conn, err := grpc.NewClient(
				address,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithStreamInterceptor(tracing.StreamClientInterceptor("sppr.storage")),
				grpc.WithUnaryInterceptor(tracing.UnaryClientInterceptor("sppr.storage")),
			)
			if err != nil {
				return nil, fmt.Errorf("create %s Storage client: %w", class, err)
			}
			s.closer.AddNamed(string(class)+" Storage gRPC connection", func(context.Context) error { return conn.Close() })
			clients[class] = storageClient.NewClient(storagev1.NewDataStorageServiceClient(conn))
		}
		s.storageClients = clients
	}
	return s.storageClients, nil
}

func (s *serviceProvider) MigrationExecutor(ctx context.Context) (service.MigrationExecutor, error) {
	if s.migrationExecutor == nil {
		actions, err := s.ActionRepository(ctx)
		if err != nil {
			return nil, err
		}
		settings, err := s.SettingsRepository(ctx)
		if err != nil {
			return nil, err
		}
		pins, err := s.PinRepository(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := s.TxManager(ctx)
		if err != nil {
			return nil, err
		}
		placements, err := s.PlacementClient()
		if err != nil {
			return nil, err
		}
		storages, err := s.StorageClients()
		if err != nil {
			return nil, err
		}
		cfg := s.cfg.Migration
		s.migrationExecutor = executorService.NewExecutor(executorService.Config{
			Workers:            cfg.Workers(),
			MaxAttempts:        cfg.MaxAttempts(),
			RetryBackoff:       cfg.RetryBackoff(),
			SourceCleanupDelay: cfg.SourceCleanupDelay(),
			PollInterval:       migrationPollInterval,
		}, actions, settings, pins, tx, placements, storages, s.migrationMetrics)
	}
	return s.migrationExecutor, nil
}

func (s *serviceProvider) MigrationCleanup(ctx context.Context) (service.MigrationCleanup, error) {
	if s.migrationCleanup == nil {
		placements, err := s.PlacementClient()
		if err != nil {
			return nil, err
		}
		storages, err := s.StorageClients()
		if err != nil {
			return nil, err
		}
		actions, err := s.ActionRepository(ctx)
		if err != nil {
			return nil, err
		}
		s.migrationCleanup = cleanupService.NewService(s.cfg.Migration.CleanupInterval(), actions, placements, storages, s.migrationMetrics)
	}
	return s.migrationCleanup, nil
}
