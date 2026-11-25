package apiserver

import (
	"net/http"
	"time"

	"github.com/kontinue-sh/kontinue/internal/store"
)

type dashboardSummaryResponse struct {
	GeneratedAt   time.Time                  `json:"generatedAt"`
	SandboxCounts map[store.SandboxState]int `json:"sandboxCounts"`
	Sandboxes     []sandboxResponse          `json:"sandboxes"`
	Templates     []templateResponse         `json:"templates"`
	Volumes       []store.VolumeRecord       `json:"volumes"`
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeProblem(w, r, methodNotAllowed, nil)
		return
	}
	sandboxes, err := s.cfg.Meta.ListSandboxes(r.Context())
	if err != nil {
		writeLifecycleError(w, r, err)
		return
	}
	templates, err := s.cfg.Meta.ListTemplates(r.Context())
	if err != nil {
		writeLifecycleError(w, r, err)
		return
	}
	volumes, err := s.cfg.Meta.ListVolumes(r.Context())
	if err != nil {
		writeLifecycleError(w, r, err)
		return
	}
	counts := make(map[store.SandboxState]int)
	publicSandboxes := make([]sandboxResponse, 0, len(sandboxes))
	for i := range sandboxes {
		if !visible(r.Context(), sandboxes[i].TenantID) {
			continue
		}
		counts[sandboxes[i].State]++
		if len(publicSandboxes) < 100 {
			publicSandboxes = append(publicSandboxes, publicSandbox(&sandboxes[i]))
		}
	}
	publicTemplates := make([]templateResponse, 0, len(templates))
	for i := range templates {
		if visible(r.Context(), templates[i].TenantID) {
			publicTemplates = append(publicTemplates, publicTemplate(&templates[i]))
		}
	}
	publicVolumes := make([]store.VolumeRecord, 0, len(volumes))
	for i := range volumes {
		if visible(r.Context(), volumes[i].TenantID) {
			publicVolumes = append(publicVolumes, volumes[i])
		}
	}
	writeJSON(w, http.StatusOK, dashboardSummaryResponse{
		GeneratedAt:   time.Now().UTC(),
		SandboxCounts: counts,
		Sandboxes:     publicSandboxes,
		Templates:     publicTemplates,
		Volumes:       publicVolumes,
	})
}
