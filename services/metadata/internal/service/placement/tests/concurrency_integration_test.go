//go:build integration

package tests

import (
	"context"
	"time"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/contextx/txctx"
	"github.com/jackc/pgx/v4"
)

func (s *PlacementProtocolSuite) TestReservationAndSwitchClassLockOrder() {
	s.mustCreateBucket()
	reserveVersion := s.mustUploadHot("reserve.txt", "v1")
	switchVersion := s.mustUpload("switch.txt", "v1")
	migration := s.migrationID("switch")
	s.mustReserve(migration, switchVersion, 1, model.StorageClassCold, model.StorageClassHot)

	s.runReservationClassLockOrder(reserveVersion, func(ctx context.Context) error {
		_, err := s.placements.SwitchPlacement(ctx, migration, objectSize, md5OfCopy, time.Hour)
		return err
	})
}

func (s *PlacementProtocolSuite) TestReservationAndDeleteClassLockOrder() {
	s.mustCreateBucket()
	reserveVersion := s.mustUploadHot("reserve.txt", "v1")
	// Delete must insert orphans of both classes without acquiring HOT first.
	s.mustUploadHot("delete.txt", "hot")
	s.mustUpload("delete.txt", "cold")

	s.runReservationClassLockOrder(reserveVersion, func(ctx context.Context) error {
		_, _, err := s.objectSvc.DeleteObjectMeta(ctx, s.bucketName(), "delete.txt")
		return err
	})
}

// Hold HOT with KEY SHARE until reserve has locked COLD and is waiting for HOT.
// The other operation must then wait for COLD without first holding HOT's FK
// lock. Once the blocker releases HOT, reserve can finish and unblock it. A
// HOT-first operation instead forms a cycle with reserve after that release.
func (s *PlacementProtocolSuite) runReservationClassLockOrder(versionID string, contender func(context.Context) error) {
	s.T().Helper()
	ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
	defer cancel()

	blocker, err := s.client.DB().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	s.Require().NoError(err)
	defer blocker.Rollback(context.Background())
	_, err = blocker.Exec(ctx, `SELECT name FROM storage_classes WHERE name = 'HOT' FOR KEY SHARE`)
	s.Require().NoError(err)

	reserveTx, err := s.client.DB().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	s.Require().NoError(err)
	defer reserveTx.Rollback(context.Background())
	contenderTx, err := s.client.DB().BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	s.Require().NoError(err)
	defer contenderTx.Rollback(context.Background())
	blockerPID := int32(blocker.Conn().PgConn().PID())
	reservePID := int32(reserveTx.Conn().PgConn().PID())
	contenderPID := int32(contenderTx.Conn().PgConn().PID())

	// The service joins the injected transaction; each worker must commit it
	// itself so the other worker can finish. Join workers before rollback.
	run := func(tx pgx.Tx, action func(context.Context) error, result *error) <-chan struct{} {
		done := make(chan struct{})
		go func() {
			defer close(done)
			*result = action(txctx.InjectTx(ctx, tx))
			if *result == nil {
				*result = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(context.Background())
			}
		}()
		return done
	}
	waitBlocked := func(waiterPID, holderPID int32) {
		s.T().Helper()
		s.Require().Eventually(func() bool {
			var blocked bool
			err := s.Pool().QueryRow(ctx, `SELECT $2::int = ANY(pg_blocking_pids($1::int))`, waiterPID, holderPID).Scan(&blocked)
			return err == nil && blocked
		}, 3*time.Second, 10*time.Millisecond)
	}

	var reserveErr, contenderErr error
	migration := s.migrationID("reserve")
	reserveDone := run(reserveTx, func(ctx context.Context) error {
		_, err := s.placements.ReserveCopy(ctx, migration, versionID, 1, model.StorageClassHot, model.StorageClassCold)
		return err
	}, &reserveErr)
	defer func() { cancel(); <-reserveDone }()
	waitBlocked(reservePID, blockerPID)

	contenderDone := run(contenderTx, contender, &contenderErr)
	defer func() { cancel(); <-contenderDone }()
	waitBlocked(contenderPID, reservePID)
	s.Require().NoError(blocker.Commit(ctx))
	<-reserveDone
	<-contenderDone
	s.Require().NoError(reserveErr)
	s.Require().NoError(contenderErr)
}
