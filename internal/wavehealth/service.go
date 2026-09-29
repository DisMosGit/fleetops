package wavehealth

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	rolloutv1 "github.com/DisMosGit/fleetops/api/proto/rollout/v1"
)

// Service is the gRPC adapter over the aggregation: it validates the request, evaluates the wave,
// and maps the outcome onto status codes. It is the boundary where internals stop — a client sees
// a status and an operator-safe message, never a driver error or a collection name.
type Service struct {
	rolloutv1.UnimplementedRolloutServiceServer

	aggregator *Aggregator
	log        *slog.Logger
}

// NewService returns the RolloutService implementation evaluating waves through aggregator and
// logging its decisions to log.
func NewService(aggregator *Aggregator, log *slog.Logger) *Service {
	return &Service{aggregator: aggregator, log: log}
}

// GetWaveHealth evaluates one wave of one rollout over the configured sliding window, ending the
// window at the moment the call is served.
//
// A request missing either id is rejected before any state is read, so a malformed call costs no
// query. An unknown rollout, an unknown wave, and a wave belonging to a different rollout are all
// NOT_FOUND: a caller must not be able to tell which of the three happened, because all three mean
// the same thing to it — the pair it asked about is not a wave it may read. Every other failure is
// INTERNAL with a fixed message, and the underlying detail goes to the log, where it belongs.
//
// An empty window is not a failure: it is served as an undecided result with sample size zero, so a
// gate can distinguish "wait for evidence" from "this call broke".
func (s *Service) GetWaveHealth(ctx context.Context, req *rolloutv1.GetWaveHealthRequest) (*rolloutv1.GetWaveHealthResponse, error) {
	rolloutID, waveID := req.GetRolloutId(), req.GetWaveId()
	if rolloutID == "" || waveID == "" {
		return nil, status.Error(codes.InvalidArgument, "rollout_id and wave_id are required")
	}

	result, err := s.aggregator.Evaluate(ctx, Query{RolloutID: rolloutID, WaveID: waveID}, time.Now())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			s.log.Info("wave health query for an unknown wave",
				"rollout_id", rolloutID, "wave_id", waveID)
			return nil, status.Error(codes.NotFound, "rollout or wave not found")
		}
		s.log.Error("evaluate wave health",
			"rollout_id", rolloutID, "wave_id", waveID, "err", err)
		return nil, status.Error(codes.Internal, "wave health evaluation failed")
	}

	s.log.Info("wave health evaluated",
		"rollout_id", result.RolloutID,
		"wave_id", result.WaveID,
		"verdict", result.Verdict.String(),
		"success_ratio", result.SuccessRatio,
		"sample_size", result.SampleSize,
		"window_start", result.WindowStart,
		"window_end", result.WindowEnd,
	)

	return &rolloutv1.GetWaveHealthResponse{
		RolloutId:             result.RolloutID,
		WaveId:                result.WaveID,
		SuccessRatio:          result.SuccessRatio,
		SampleSize:            result.SampleSize,
		Verdict:               verdictProto(result.Verdict),
		WindowStart:           timestamppb.New(result.WindowStart),
		WindowEnd:             timestamppb.New(result.WindowEnd),
		SampleHealthThreshold: result.Settings.SampleHealthThreshold,
		MinSuccessRatio:       result.Settings.MinSuccessRatio,
		MinSamples:            int64(result.Settings.MinSamples),
	}, nil
}

// verdictProto maps the domain verdict onto the wire enum, defaulting to undecided. The mapping is
// explicit rather than a cast because the two zero values mean different things: the domain's zero
// is "not enough evidence", while the wire's zero is "unset", which a served response never carries.
func verdictProto(verdict Verdict) rolloutv1.WaveHealthVerdict {
	switch verdict {
	case VerdictHealthy:
		return rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_HEALTHY
	case VerdictUnhealthy:
		return rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNHEALTHY
	default:
		return rolloutv1.WaveHealthVerdict_WAVE_HEALTH_VERDICT_UNDECIDED
	}
}
