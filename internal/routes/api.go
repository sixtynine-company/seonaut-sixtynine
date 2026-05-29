package routes

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/stjudewashere/seonaut/internal/services"
)

// apiHandler serves the internal JSON crawl API. It embeds the service Container
// so it can reach the APICrawlService and the loaded config.
type apiHandler struct {
	*services.Container
}

// apiCrawlRequest is the JSON body accepted by the start-crawl endpoint.
type apiCrawlRequest struct {
	URL            string `json:"url"`
	Depth          string `json:"depth"`
	MaxPages       int    `json:"maxPages"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
}

// apiCrawlAccepted is the response returned when a crawl is accepted.
type apiCrawlAccepted struct {
	CrawlId int64  `json:"crawlId"`
	Status  string `json:"status"`
}

// startCrawlHandler accepts a crawl request, enqueues it and returns the crawl
// id and lifecycle status. A malformed body or a missing URL is a 400; an
// unknown depth or unsupported protocol is also a 400; anything else is a 500.
// On success it responds with 202 Accepted.
func (h apiHandler) startCrawlHandler(w http.ResponseWriter, r *http.Request) {
	var req apiCrawlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	if req.URL == "" {
		writeAPIError(w, http.StatusBadRequest, "url is required")
		return
	}

	id, status, err := h.APICrawlService.Enqueue(req.URL, req.Depth, req.MaxPages, req.TimeoutSeconds)
	if err != nil {
		if errors.Is(err, services.ErrUnknownDepth) || errors.Is(err, services.ErrProtocolNotSupported) {
			writeAPIError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, apiCrawlAccepted{CrawlId: id, Status: status})
}

// statusHandler returns the live status of a tracked crawl. A non-numeric id is
// a 400 and an unknown crawl id is a 404.
func (h apiHandler) statusHandler(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("crawlId")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid crawl id")
		return
	}

	st, ok := h.APICrawlService.Status(id)
	if !ok {
		writeAPIError(w, http.StatusNotFound, "crawl not found")
		return
	}

	writeJSON(w, http.StatusOK, st)
}

// resultsHandler returns the full results of a finished crawl. An unknown crawl
// id is a 404, a crawl that has not finished yet is a 409 (with the current
// status carried in the error message), and anything else is a 500.
func (h apiHandler) resultsHandler(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("crawlId")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid crawl id")
		return
	}

	res, err := h.APICrawlService.Results(id)
	if err != nil {
		switch {
		case errors.Is(err, services.ErrCrawlNotFound):
			writeAPIError(w, http.StatusNotFound, err.Error())
		case errors.Is(err, services.ErrCrawlNotFinished):
			writeAPIError(w, http.StatusConflict, err.Error())
		default:
			writeAPIError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	writeJSON(w, http.StatusOK, res)
}

// apiKeyAuth wraps a handler with X-API-Key authentication. It fails closed:
// when no key is configured every request is rejected. The supplied key is
// compared against the configured key in constant time.
func (h apiHandler) apiKeyAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		configured := ""
		if h.Config.API != nil {
			configured = h.Config.API.Key
		}

		// Fail closed: an unset key disables the API entirely.
		if configured == "" {
			writeAPIError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		provided := r.Header.Get("X-API-Key")
		if subtle.ConstantTimeCompare([]byte(provided), []byte(configured)) != 1 {
			writeAPIError(w, http.StatusUnauthorized, "unauthorized")
			return
		}

		next(w, r)
	}
}

// apiError is the JSON shape returned for every API error.
type apiError struct {
	Error string `json:"error"`
}

// writeJSON writes v as a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeAPIError writes a JSON error response with the given status code.
func writeAPIError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiError{Error: msg})
}
