// Package testkit holds the in-process Metadata and Storage gRPC fakes shared by Gateway tests.
// It is imported by tests only.
package testkit

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/client"
	metadataclient "github.com/alesplll/opens3-rebac/services/gateway/internal/client/metadata"
	storageclient "github.com/alesplll/opens3-rebac/services/gateway/internal/client/storage"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/service"
	objectservice "github.com/alesplll/opens3-rebac/services/gateway/internal/service/object"
	accessproducer "github.com/alesplll/opens3-rebac/services/gateway/internal/service/producer/access"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	metadatav1 "github.com/alesplll/opens3-rebac/shared/pkg/go/metadata/v1"
	storagev1 "github.com/alesplll/opens3-rebac/shared/pkg/go/storage/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const (
	// Bucket is the only bucket the fake Metadata knows.
	Bucket = "fixture"
	// RetrieveChunkSize is the size of one RetrieveObject message and of the Gateway's receive limit.
	RetrieveChunkSize = 1 << 20
	// MetadataTimeout is the per-RPC timeout of clients built by this package.
	MetadataTimeout = 5 * time.Second

	// Metadata RPC names passed to OnMetadata.
	OpHeadBucket          = "HeadBucket"
	OpChooseWriteClass    = "ChooseWriteClass"
	OpCreateObjectVersion = "CreateObjectVersion"
	OpGetObjectMeta       = "GetObjectMeta"
	OpDeleteObjectMeta    = "DeleteObjectMeta"
)

// Upstream is one fake of both downstream services. State fields must be set before traffic starts,
// or under the embedded mutex.
type Upstream struct {
	sync.Mutex
	Blobs      map[string][]byte
	BlobClass  map[string]model.StorageClass                // Storage instance that holds each blob; missing means COLD
	Meta       map[string]*metadatav1.GetObjectMetaResponse // by object key
	StoreCount int
	// WriteClass is the answer of ChooseWriteClass; empty means COLD.
	WriteClass model.StorageClass
	// WriteSizes records the size passed to every ChooseWriteClass; nil means unknown.
	WriteSizes []*int64

	FailStore    error
	FailRetrieve error // returned after the last chunk
	// OnMetadata runs at the start of every Metadata RPC; a non-nil result is returned to the caller.
	OnMetadata func(ctx context.Context, op string) error
	OnChunk    func()
	// BeforeCommit runs after the upload is received and before the blob is stored.
	BeforeCommit func(ctx context.Context) error
	// BeforeNextChunk runs before every RetrieveObject chunk except the first.
	BeforeNextChunk func(ctx context.Context) error
}

func New() *Upstream {
	return &Upstream{
		Blobs:     make(map[string][]byte),
		BlobClass: make(map[string]model.StorageClass),
		Meta:      make(map[string]*metadatav1.GetObjectMetaResponse),
	}
}

// FailMetadata makes the listed Metadata RPCs (all when none are listed) return err.
func (u *Upstream) FailMetadata(err error, ops ...string) {
	u.OnMetadata = func(_ context.Context, op string) error {
		if len(ops) == 0 || slices.Contains(ops, op) {
			return err
		}
		return nil
	}
}

// Seed registers a stored COLD object that Metadata and Storage both know.
func (u *Upstream) Seed(key string, data []byte) {
	u.SeedIn(model.StorageClassCold, key, data)
}

// SeedIn registers a stored object placed in the given class.
func (u *Upstream) SeedIn(class model.StorageClass, key string, data []byte) {
	u.Lock()
	defer u.Unlock()
	blobID := "seed-" + key
	u.Blobs[blobID] = data
	u.BlobClass[blobID] = class
	u.Meta[key] = &metadatav1.GetObjectMetaResponse{
		BlobId:       blobID,
		VersionId:    "version-1",
		SizeBytes:    int64(len(data)),
		StorageClass: string(class),
	}
}

func (u *Upstream) classOf(blobID string) model.StorageClass {
	if class, ok := u.BlobClass[blobID]; ok {
		return class
	}
	return model.StorageClassCold
}

// Stats returns the number of stored blobs and published versions.
func (u *Upstream) Stats() (blobs, versions int) {
	u.Lock()
	defer u.Unlock()
	return u.StoreCount, len(u.Meta)
}

// Register serves Metadata and the COLD Storage on one server.
func (u *Upstream) Register(registrar grpc.ServiceRegistrar) {
	metadatav1.RegisterMetadataServiceServer(registrar, &metadataServer{u: u})
	storagev1.RegisterDataStorageServiceServer(registrar, &storageServer{u: u, class: model.StorageClassCold})
}

func (u *Upstream) registerStorage(class model.StorageClass) func(grpc.ServiceRegistrar) {
	return func(registrar grpc.ServiceRegistrar) {
		storagev1.RegisterDataStorageServiceServer(registrar, &storageServer{u: u, class: class})
	}
}

// Dial serves Metadata and the COLD Storage over an in-memory listener and returns a client connection to them.
func (u *Upstream) Dial(t *testing.T) *grpc.ClientConn {
	t.Helper()
	return dial(t, u.Register)
}

// DialStorage serves only the Storage of the given class.
func (u *Upstream) DialStorage(t *testing.T, class model.StorageClass) *grpc.ClientConn {
	t.Helper()
	return dial(t, u.registerStorage(class))
}

func dial(t *testing.T, register func(grpc.ServiceRegistrar)) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// ListenTCP serves Metadata and the COLD Storage on a loopback port, for tests that go through the real app wiring.
func (u *Upstream) ListenTCP(t *testing.T) string {
	t.Helper()
	return listenTCP(t, u.Register)
}

// ListenStorageTCP serves only the Storage of the given class on a loopback port.
func (u *Upstream) ListenStorageTCP(t *testing.T, class model.StorageClass) string {
	t.Helper()
	return listenTCP(t, u.registerStorage(class))
}

func listenTCP(t *testing.T, register func(grpc.ServiceRegistrar)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// Clients returns the Metadata client and the COLD Storage client.
func (u *Upstream) Clients(t *testing.T) (client.MetadataClient, client.StorageClient) {
	t.Helper()
	conn := u.Dial(t)
	return metadataclient.NewClient(metadatav1.NewMetadataServiceClient(conn), MetadataTimeout),
		storageclient.NewClient(storagev1.NewDataStorageServiceClient(conn), RetrieveChunkSize)
}

func (u *Upstream) ObjectService(t *testing.T) service.ObjectService {
	t.Helper()
	logger.SetNopLogger()
	meta, cold := u.Clients(t)
	hot := storageclient.NewClient(storagev1.NewDataStorageServiceClient(u.DialStorage(t, model.StorageClassHot)), RetrieveChunkSize)
	return objectservice.NewService(meta, map[model.StorageClass]client.StorageClient{
		model.StorageClassHot:  hot,
		model.StorageClassCold: cold,
	}, accessproducer.NewNoopService())
}

type metadataServer struct {
	metadatav1.UnimplementedMetadataServiceServer
	u *Upstream
}

func (s *metadataServer) hook(ctx context.Context, op string) error {
	if s.u.OnMetadata == nil {
		return nil
	}
	return s.u.OnMetadata(ctx, op)
}

func (s *metadataServer) HeadBucket(ctx context.Context, req *metadatav1.HeadBucketRequest) (*metadatav1.HeadBucketResponse, error) {
	if err := s.hook(ctx, OpHeadBucket); err != nil {
		return nil, err
	}
	return &metadatav1.HeadBucketResponse{Exists: req.GetBucketName() == Bucket}, nil
}

func (s *metadataServer) ChooseWriteClass(ctx context.Context, req *metadatav1.ChooseWriteClassRequest) (*metadatav1.ChooseWriteClassResponse, error) {
	if err := s.hook(ctx, OpChooseWriteClass); err != nil {
		return nil, err
	}
	s.u.Lock()
	defer s.u.Unlock()
	s.u.WriteSizes = append(s.u.WriteSizes, req.SizeBytes)
	class := s.u.WriteClass
	if class == "" {
		class = model.StorageClassCold
	}
	return &metadatav1.ChooseWriteClassResponse{StorageClass: string(class)}, nil
}

func (s *metadataServer) CreateObjectVersion(ctx context.Context, req *metadatav1.CreateObjectVersionRequest) (*metadatav1.CreateObjectVersionResponse, error) {
	if err := s.hook(ctx, OpCreateObjectVersion); err != nil {
		return nil, err
	}
	s.u.Lock()
	defer s.u.Unlock()
	if _, ok := s.u.Blobs[req.GetBlobId()]; !ok {
		return nil, status.Error(codes.FailedPrecondition, "metadata published before blob commit")
	}
	if string(s.u.classOf(req.GetBlobId())) != req.GetStorageClass() {
		return nil, status.Error(codes.FailedPrecondition, "blob is not in the declared storage class")
	}
	s.u.Meta[req.GetKey()] = &metadatav1.GetObjectMetaResponse{
		BlobId:       req.GetBlobId(),
		VersionId:    "version-1",
		SizeBytes:    req.GetSizeBytes(),
		Etag:         req.GetEtag(),
		ContentType:  req.GetContentType(),
		StorageClass: req.GetStorageClass(),
	}
	return &metadatav1.CreateObjectVersionResponse{VersionId: "version-1"}, nil
}

func (s *metadataServer) GetObjectMeta(ctx context.Context, req *metadatav1.GetObjectMetaRequest) (*metadatav1.GetObjectMetaResponse, error) {
	if err := s.hook(ctx, OpGetObjectMeta); err != nil {
		return nil, err
	}
	s.u.Lock()
	defer s.u.Unlock()
	if req.GetBucketName() != Bucket || s.u.Meta[req.GetKey()] == nil {
		return nil, status.Error(codes.NotFound, "object not found")
	}
	return s.u.Meta[req.GetKey()], nil
}

func (s *metadataServer) DeleteObjectMeta(ctx context.Context, req *metadatav1.DeleteObjectMetaRequest) (*metadatav1.DeleteObjectMetaResponse, error) {
	if err := s.hook(ctx, OpDeleteObjectMeta); err != nil {
		return nil, err
	}
	s.u.Lock()
	defer s.u.Unlock()
	meta := s.u.Meta[req.GetKey()]
	if req.GetBucketName() != Bucket || meta == nil {
		return nil, status.Error(codes.NotFound, "object not found")
	}
	delete(s.u.Meta, req.GetKey())
	return &metadatav1.DeleteObjectMetaResponse{BlobId: meta.GetBlobId(), Success: true}, nil
}

// storageServer is the Storage instance of one class: it sees only blobs written to that class.
type storageServer struct {
	storagev1.UnimplementedDataStorageServiceServer
	u     *Upstream
	class model.StorageClass
}

func (s *storageServer) StoreObject(stream storagev1.DataStorageService_StoreObjectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetHeader() == nil {
		return status.Error(codes.InvalidArgument, "missing header")
	}
	if s.u.FailStore != nil {
		return s.u.FailStore
	}
	var content bytes.Buffer
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		content.Write(message.GetChunk().GetData())
		if s.u.OnChunk != nil {
			s.u.OnChunk()
		}
	}
	if first.GetHeader().Size != nil && *first.GetHeader().Size != int64(content.Len()) {
		return status.Error(codes.InvalidArgument, "size mismatch")
	}
	if s.u.BeforeCommit != nil {
		if err := s.u.BeforeCommit(stream.Context()); err != nil {
			return err
		}
	}
	s.u.Lock()
	s.u.StoreCount++
	blobID := fmt.Sprintf("blob-%d", s.u.StoreCount)
	s.u.Blobs[blobID] = bytes.Clone(content.Bytes())
	s.u.BlobClass[blobID] = s.class
	s.u.Unlock()
	return stream.SendAndClose(&storagev1.StoreObjectResponse{BlobId: blobID, ChecksumMd5: fmt.Sprintf("%x", md5.Sum(content.Bytes()))})
}

func (s *storageServer) RetrieveObject(req *storagev1.RetrieveObjectRequest, stream storagev1.DataStorageService_RetrieveObjectServer) error {
	s.u.Lock()
	data, ok := s.u.Blobs[req.GetBlobId()]
	ok = ok && s.u.classOf(req.GetBlobId()) == s.class
	s.u.Unlock()
	if !ok {
		return status.Error(codes.NotFound, "blob not found")
	}
	totalSize := int64(len(data))
	if req.GetOffset() != 0 || req.GetLength() != 0 {
		start := min(req.GetOffset(), totalSize)
		end := totalSize
		if req.GetLength() > 0 {
			end = min(start+req.GetLength(), totalSize)
		}
		data = data[start:end]
	}
	if len(data) == 0 {
		return s.u.FailRetrieve
	}
	for offset := 0; offset < len(data); offset += RetrieveChunkSize {
		if offset > 0 && s.u.BeforeNextChunk != nil {
			if err := s.u.BeforeNextChunk(stream.Context()); err != nil {
				return err
			}
		}
		end := min(offset+RetrieveChunkSize, len(data))
		if err := stream.Send(&storagev1.RetrieveObjectResponse{Data: data[offset:end], TotalSize: totalSize}); err != nil {
			return err
		}
	}
	return s.u.FailRetrieve
}
