package p2p

import (
	"context"

	"github.com/kontinue-sh/kontinue/pkg/protocol"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ChunkProvider is satisfied by ArenaManager to serve local L1/L2 memory chunks to peer worker nodes.
type ChunkProvider interface {
	LoadLocalManifest(snapshotID string) (*protocol.SnapshotManifest, error)
	ReadLocalChunk(blake3Hash string) ([]byte, error)
}

// Server implements protocol.NodeArenaP2PServer for low-latency cross-node chunk streaming.
type Server struct {
	provider ChunkProvider
}

func NewServer(provider ChunkProvider) *Server {
	return &Server{provider: provider}
}

func (s *Server) GetManifest(ctx context.Context, req *protocol.GetManifestRequest) (*protocol.GetManifestResponse, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinued-p2p", "p2p.get_manifest",
		attribute.String(telemetry.AttrSnapshotID, req.SnapshotID),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	m, err := s.provider.LoadLocalManifest(req.SnapshotID)
	if err != nil {
		retErr = status.Errorf(codes.NotFound, "manifest %s not cached on peer: %v", req.SnapshotID, err)
		return nil, retErr
	}
	return &protocol.GetManifestResponse{Manifest: *m}, nil
}

func (s *Server) GetChunk(ctx context.Context, req *protocol.GetChunkRequest) (*protocol.GetChunkResponse, error) {
	ctx, span := telemetry.StartSpan(ctx, "kontinued-p2p", "p2p.get_chunk",
		attribute.String(telemetry.AttrChunkHash, req.Blake3Hash),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()

	data, err := s.provider.ReadLocalChunk(req.Blake3Hash)
	if err != nil {
		retErr = status.Errorf(codes.NotFound, "chunk %s not found on peer: %v", req.Blake3Hash, err)
		return nil, retErr
	}
	span.SetAttributes(attribute.Int(telemetry.AttrBytes, len(data)))
	return &protocol.GetChunkResponse{
		Blake3Hash: req.Blake3Hash,
		Data:       data,
	}, nil
}

func (s *Server) StreamChunks(req *protocol.StreamChunksRequest, stream protocol.NodeArenaStreamServer) error {
	ctx, span := telemetry.StartSpan(stream.Context(), "kontinued-p2p", "p2p.stream_chunks",
		attribute.Int("chunk.requested_count", len(req.Blake3Hashes)),
	)
	var retErr error
	defer func() { telemetry.EndSpan(span, retErr) }()
	_ = ctx

	sent := 0
	for _, h := range req.Blake3Hashes {
		data, err := s.provider.ReadLocalChunk(h)
		if err != nil {
			continue
		}
		if err := stream.Send(&protocol.ChunkFrame{
			Blake3Hash: h,
			Data:       data,
		}); err != nil {
			retErr = err
			return err
		}
		sent++
	}
	span.SetAttributes(attribute.Int("chunk.sent_count", sent))
	return nil
}
