package apiserver

import (
	"errors"
	"log"
	"net/http"

	"github.com/kontinue-sh/kontinue/internal/store"
	"github.com/kontinue-sh/kontinue/pkg/telemetry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type publicError struct {
	Code    string `json:"code"`
	Message string `json:"error"`
}

type lifecycleProblem struct {
	status int
	body   publicError
}

var (
	invalidJSON              = lifecycleProblem{http.StatusBadRequest, publicError{"invalid_request", "request body must be valid JSON"}}
	missingSandboxID         = lifecycleProblem{http.StatusBadRequest, publicError{"invalid_request", "missing sandbox id"}}
	tenantCredentialRequired = lifecycleProblem{http.StatusUnauthorized, publicError{"unauthorized", "tenant credential required"}}
	invalidSandboxCredential = lifecycleProblem{http.StatusUnauthorized, publicError{"unauthorized", "invalid sandbox credential"}}
	resourceNotFound         = lifecycleProblem{http.StatusNotFound, publicError{"not_found", "resource not found"}}
	templateNotFound         = lifecycleProblem{http.StatusNotFound, publicError{"not_found", "template not found"}}
	sandboxNotFound          = lifecycleProblem{http.StatusNotFound, publicError{"not_found", "sandbox not found"}}
	unknownSandboxAction     = lifecycleProblem{http.StatusNotFound, publicError{"not_found", "unknown sandbox action"}}
	methodNotAllowed         = lifecycleProblem{http.StatusMethodNotAllowed, publicError{"method_not_allowed", "method not allowed"}}
	sandboxAlreadyExists     = lifecycleProblem{http.StatusConflict, publicError{"sandbox_exists", "sandbox ID is already in use"}}
	transitionInProgress     = lifecycleProblem{http.StatusConflict, publicError{"transition_in_progress", "a lifecycle transition is already in progress"}}
	staleExecution           = lifecycleProblem{http.StatusConflict, publicError{"stale_execution", "the execution has changed"}}
	noCarrierCapacity        = lifecycleProblem{http.StatusServiceUnavailable, publicError{"capacity_unavailable", "no Carrier capacity is currently available"}}
	workerUnavailable        = lifecycleProblem{http.StatusServiceUnavailable, publicError{"worker_unavailable", "a worker is temporarily unavailable"}}
	lifecycleInternalError   = lifecycleProblem{http.StatusInternalServerError, publicError{"internal_error", "the lifecycle request could not be completed"}}
)

func writeLifecycleError(w http.ResponseWriter, r *http.Request, err error) {
	var problem lifecycleProblem
	switch {
	case errors.Is(err, errOtherTenant), errors.Is(err, store.ErrNotFound):
		problem = resourceNotFound
	case errors.Is(err, errCredential):
		problem = invalidSandboxCredential
	case errors.Is(err, store.ErrSandboxExists):
		problem = sandboxAlreadyExists
	case errors.Is(err, store.ErrVolumeExists):
		problem = lifecycleProblem{http.StatusConflict, publicError{"volume_exists", "volume ID is already in use"}}
	case errors.Is(err, store.ErrTransitionLocked):
		problem = transitionInProgress
	case errors.Is(err, store.ErrStaleExecutionFence):
		problem = staleExecution
	case errors.Is(err, store.ErrNoIdleCarrierSlot):
		problem = noCarrierCapacity
	case status.Code(err) == codes.Unavailable:
		problem = workerUnavailable
	default:
		problem = lifecycleInternalError
	}
	writeProblem(w, r, problem, err)
}

func writeProblem(w http.ResponseWriter, r *http.Request, problem lifecycleProblem, err error) {
	if err != nil {
		log.Printf("lifecycle request failed method=%q path=%q trace_id=%q status=%d code=%s detail=%q",
			r.Method, r.URL.Path, telemetry.TraceIDFromContext(r.Context()), problem.status, problem.body.Code, err)
	}
	writeJSON(w, problem.status, problem.body)
}
