package http

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/transport"
	"k8s.io/streaming/pkg/httpstream"

	"github.com/argoproj/argo-cd/v3/common"
	"github.com/argoproj/argo-cd/v3/util/env"
)

const (
	maxCookieLength = 4093

	// limit size of the resp to 512KB
	respReadLimit       = int64(524288)
	retryWaitMax        = time.Duration(10) * time.Second
	EnvRetryMax         = "ARGOCD_K8SCLIENT_RETRY_MAX"
	EnvRetryBaseBackoff = "ARGOCD_K8SCLIENT_RETRY_BASE_BACKOFF"

	EnvSlowRequestLogThreshold     = "ARGOCD_K8SCLIENT_SLOW_REQUEST_LOG_THRESHOLD"
	DefaultSlowRequestLogThreshold = 30 * time.Second
)

// max number of chunks a cookie can be broken into. To be compatible with
// widest range of browsers, you shouldn't create more than 30 cookies per domain
var maxCookieNumber = env.ParseNumFromEnv(common.EnvMaxCookieNumber, 20, 0, math.MaxInt)

// MakeCookieMetadata generates a string representing a Web cookie.  Yum!
func MakeCookieMetadata(key, value string, flags ...string) ([]string, error) {
	attributes := strings.Join(flags, "; ")

	// cookie: name=value; attributes and key: key-(i) e.g. argocd.token-1
	maxValueLength := maxCookieValueLength(key, attributes)
	numberOfCookies := int(math.Ceil(float64(len(value)) / float64(maxValueLength)))
	if numberOfCookies > maxCookieNumber {
		return nil, fmt.Errorf("the authentication token is %d characters long and requires %d cookies but the max number of cookies is %d. Contact your Argo CD administrator to increase the max number of cookies", len(value), numberOfCookies, maxCookieNumber)
	}

	return splitCookie(key, value, attributes), nil
}

// browser has limit on size of cookie, currently 4kb. In order to
// support cookies longer than 4kb, we split cookie into multiple 4kb chunks.
// first chunk will be of format argocd.token=<numberOfChunks>:token; attributes
func splitCookie(key, value, attributes string) []string {
	var cookies []string
	valueLength := len(value)
	// cookie: name=value; attributes and key: key-(i) e.g. argocd.token-1
	maxValueLength := maxCookieValueLength(key, attributes)
	numberOfChunks := int(math.Ceil(float64(valueLength) / float64(maxValueLength)))

	var end int
	for i, j := 0, 0; i < valueLength; i, j = i+maxValueLength, j+1 {
		end = min(i+maxValueLength, valueLength)

		var cookie string
		switch {
		case j == 0 && numberOfChunks == 1:
			cookie = fmt.Sprintf("%s=%s", key, value[i:end])
		case j == 0:
			cookie = fmt.Sprintf("%s=%d:%s", key, numberOfChunks, value[i:end])
		default:
			cookie = fmt.Sprintf("%s-%d=%s", key, j, value[i:end])
		}
		if attributes != "" {
			cookie = fmt.Sprintf("%s; %s", cookie, attributes)
		}
		cookies = append(cookies, cookie)
	}
	return cookies
}

// JoinCookies combines chunks of cookie based on key as prefix. It returns cookie
// value as string. cookieString is of format key1=value1; key2=value2; key3=value3
// first chunk will be of format argocd.token=<numberOfChunks>:token; attributes
func JoinCookies(key string, cookieList []*http.Cookie) (string, error) {
	cookies := make(map[string]string)
	for _, cookie := range cookieList {
		if !strings.HasPrefix(cookie.Name, key) {
			continue
		}
		cookies[cookie.Name] = cookie.Value
	}

	var sb strings.Builder
	var numOfChunks int
	var err error
	var token string
	var ok bool

	if token, ok = cookies[key]; !ok {
		return "", fmt.Errorf("failed to retrieve cookie %s", key)
	}
	parts := strings.Split(token, ":")

	switch len(parts) {
	case 2:
		if numOfChunks, err = strconv.Atoi(parts[0]); err != nil {
			return "", err
		}
		sb.WriteString(parts[1])
	case 1:
		numOfChunks = 1
		sb.WriteString(parts[0])
	default:
		return "", fmt.Errorf("invalid cookie for key %s", key)
	}

	for i := 1; i < numOfChunks; i++ {
		sb.WriteString(cookies[fmt.Sprintf("%s-%d", key, i)])
	}
	return sb.String(), nil
}

func maxCookieValueLength(key, attributes string) int {
	if attributes != "" {
		return maxCookieLength - (len(key) + 3) - (len(attributes) + 2)
	}
	return maxCookieLength - (len(key) + 3)
}

// DebugTransport is a HTTP Client Transport to enable debugging
type DebugTransport struct {
	T http.RoundTripper
}

func (d DebugTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	reqDump, err := httputil.DumpRequest(req, true)
	if err != nil {
		return nil, err
	}
	log.Printf("%s", reqDump)

	resp, err := d.T.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	respDump, err := httputil.DumpResponse(resp, true)
	if err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	log.Printf("%s", respDump)
	return resp, nil
}

// TransportWithHeader is a HTTP Client Transport with default headers.
type TransportWithHeader struct {
	RoundTripper http.RoundTripper
	Header       http.Header
}

func (rt *TransportWithHeader) RoundTrip(r *http.Request) (*http.Response, error) {
	if rt.Header != nil {
		headers := rt.Header.Clone()
		for k, vs := range r.Header {
			for _, v := range vs {
				headers.Add(k, v)
			}
		}
		r.Header = headers
	}
	return rt.RoundTripper.RoundTrip(r)
}

func WithRetry(maxRetries int64, baseRetryBackoff time.Duration) transport.WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &retryTransport{
			inner:      rt,
			maxRetries: maxRetries,
			backoff:    baseRetryBackoff,
		}
	}
}

// WithServerSideTimeout adds the timeout query parameter understood by the
// Kubernetes API server without imposing a client-side deadline.
func WithServerSideTimeout(timeout time.Duration) transport.WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		if timeout <= 0 {
			return rt
		}
		if rt == nil {
			rt = http.DefaultTransport
		}
		return &serverSideTimeoutTransport{
			inner:   rt,
			timeout: timeout,
		}
	}
}

type serverSideTimeoutTransport struct {
	inner   http.RoundTripper
	timeout time.Duration
}

var _ utilnet.RoundTripperWrapper = (*serverSideTimeoutTransport)(nil)

func (t *serverSideTimeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	query := req.URL.Query()
	if query.Get("timeout") != "" {
		return t.inner.RoundTrip(req)
	}

	clonedReq := req.Clone(req.Context())
	query.Set("timeout", t.timeout.String())
	clonedReq.URL.RawQuery = query.Encode()
	return t.inner.RoundTrip(clonedReq)
}

func (t *serverSideTimeoutTransport) WrappedRoundTripper() http.RoundTripper {
	return t.inner
}

// WithSlowRequestLogging logs Kubernetes API requests that spend longer than
// threshold waiting for response headers or stop making progress while reading
// a finite response body. It observes requests without cancelling them.
func WithSlowRequestLogging(threshold time.Duration) transport.WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		if threshold <= 0 {
			return rt
		}
		if rt == nil {
			rt = http.DefaultTransport
		}
		return newSlowRequestLoggingTransport(rt, threshold, logSlowRequest)
	}
}

type slowRequestLogger func(log.Fields, log.Level, string)

func logSlowRequest(fields log.Fields, level log.Level, message string) {
	log.WithFields(fields).Log(level, message)
}

type slowRequestLoggingTransport struct {
	inner     http.RoundTripper
	threshold time.Duration
	logger    slowRequestLogger
}

var (
	_                    utilnet.RoundTripperWrapper = (*slowRequestLoggingTransport)(nil)
	slowRequestIDCounter atomic.Uint64
)

func newSlowRequestLoggingTransport(rt http.RoundTripper, threshold time.Duration, logger slowRequestLogger) http.RoundTripper {
	return &slowRequestLoggingTransport{
		inner:     rt,
		threshold: threshold,
		logger:    logger,
	}
}

func (t *slowRequestLoggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	state := newSlowRequestLogState(req, t.logger)
	headerTimer := time.AfterFunc(t.threshold, state.logWaitingForHeaders)

	resp, err := t.inner.RoundTrip(req)
	state.markRoundTripDone(resp)
	headerTimer.Stop()

	if err != nil {
		state.logCompletion("request_failed", 0, false, err)
		return nil, err
	}

	if resp.Body == nil || resp.Body == http.NoBody || resp.ContentLength == 0 || isStreamingResponse(req, resp) {
		state.logCompletion("response_headers_received", 0, false, nil)
		return resp, nil
	}

	resp.Body = newSlowResponseBody(resp.Body, t.threshold, state)
	return resp, nil
}

func (t *slowRequestLoggingTransport) WrappedRoundTripper() http.RoundTripper {
	return t.inner
}

type slowRequestLogState struct {
	mu sync.Mutex

	logger slowRequestLogger

	requestID uint64
	method    string
	host      string
	path      string
	startedAt time.Time

	roundTripDone bool
	headersWarned bool
	timeToHeaders time.Duration
	statusCode    int
}

func newSlowRequestLogState(req *http.Request, logger slowRequestLogger) *slowRequestLogState {
	return &slowRequestLogState{
		logger:    logger,
		requestID: slowRequestIDCounter.Add(1),
		method:    req.Method,
		host:      req.URL.Host,
		path:      req.URL.EscapedPath(),
		startedAt: time.Now(),
	}
}

func (s *slowRequestLogState) logWaitingForHeaders() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.roundTripDone {
		return
	}

	s.headersWarned = true
	fields := s.fieldsLocked("waiting_for_response_headers")
	s.logger(fields, log.WarnLevel, "Kubernetes API request is still waiting for response headers")
}

func (s *slowRequestLogState) markRoundTripDone(resp *http.Response) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roundTripDone = true
	if resp != nil {
		s.timeToHeaders = time.Since(s.startedAt)
		s.statusCode = resp.StatusCode
	}
}

func (s *slowRequestLogState) fieldsLocked(phase string) log.Fields {
	fields := log.Fields{
		"request_id": s.requestID,
		"method":     s.method,
		"host":       s.host,
		"path":       s.path,
		"command":    fmt.Sprintf("%s %s", s.method, s.path),
		"phase":      phase,
		"elapsed_ms": time.Since(s.startedAt).Milliseconds(),
	}
	if s.statusCode != 0 {
		fields["status_code"] = s.statusCode
	}
	if s.timeToHeaders != 0 {
		fields["time_to_headers_ms"] = s.timeToHeaders.Milliseconds()
	}
	return fields
}

func (s *slowRequestLogState) logBodyStalled(idleFor time.Duration, bytesReceived int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fields := s.fieldsLocked("reading_response_body")
	fields["body_idle_ms"] = idleFor.Milliseconds()
	fields["bytes_received"] = bytesReceived
	s.logger(fields, log.WarnLevel, "Kubernetes API response body has stopped making progress")
}

func (s *slowRequestLogState) logCompletion(result string, bytesReceived int64, bodyWarned bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.headersWarned && !bodyWarned {
		return
	}

	fields := s.fieldsLocked("request_completed")
	fields["result"] = result
	fields["bytes_received"] = bytesReceived
	if err != nil {
		fields["error"] = err
	}
	s.logger(fields, log.InfoLevel, "Slow Kubernetes API request completed")
}

type slowResponseBody struct {
	inner     io.ReadCloser
	threshold time.Duration
	state     *slowRequestLogState

	mu            sync.Mutex
	timer         *time.Timer
	lastProgress  time.Time
	bytesReceived int64
	warned        bool
	done          bool
}

func newSlowResponseBody(inner io.ReadCloser, threshold time.Duration, state *slowRequestLogState) io.ReadCloser {
	body := &slowResponseBody{
		inner:        inner,
		threshold:    threshold,
		state:        state,
		lastProgress: time.Now(),
	}
	body.timer = time.AfterFunc(threshold, body.logStalled)
	return body
}

func (b *slowResponseBody) Read(p []byte) (int, error) {
	n, err := b.inner.Read(p)

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return n, err
	}

	if n > 0 {
		b.bytesReceived += int64(n)
		b.lastProgress = time.Now()
		if !b.warned {
			b.timer.Stop()
			b.timer.Reset(b.threshold)
		}
	}

	if err != nil {
		result := "response_body_failed"
		completionErr := err
		if err == io.EOF {
			result = "response_body_complete"
			completionErr = nil
		}
		b.finishLocked(result, completionErr)
	}
	return n, err
}

func (b *slowResponseBody) Close() error {
	err := b.inner.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.done {
		b.finishLocked("response_body_closed", err)
	}
	return err
}

func (b *slowResponseBody) logStalled() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done || b.warned {
		return
	}

	b.warned = true
	b.state.logBodyStalled(time.Since(b.lastProgress), b.bytesReceived)
}

func (b *slowResponseBody) finishLocked(result string, err error) {
	b.done = true
	b.timer.Stop()
	b.state.logCompletion(result, b.bytesReceived, b.warned, err)
}

func isStreamingResponse(req *http.Request, resp *http.Response) bool {
	query := req.URL.Query()
	watch, _ := strconv.ParseBool(query.Get("watch"))
	follow, _ := strconv.ParseBool(query.Get("follow"))
	return watch ||
		(follow && strings.HasSuffix(req.URL.Path, "/log")) ||
		httpstream.IsUpgradeRequest(req) ||
		resp.StatusCode == http.StatusSwitchingProtocols ||
		strings.Contains(resp.Header.Get("Content-Type"), "stream=watch")
}

type retryTransport struct {
	inner      http.RoundTripper
	maxRetries int64
	backoff    time.Duration
}

func isRetriable(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	if resp.StatusCode == 0 || (resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented) {
		return true
	}
	return false
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	var err error
	backoff := t.backoff
	var bodyBytes []byte
	if req.Body != nil {
		bodyBytes, _ = io.ReadAll(req.Body)
	}
	for i := 0; i <= int(t.maxRetries); i++ {
		req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
		resp, err = t.inner.RoundTrip(req)
		if i < int(t.maxRetries) && (err != nil || isRetriable(resp)) {
			if resp != nil && resp.Body != nil {
				drainBody(resp.Body)
			}
			if backoff > retryWaitMax {
				backoff = retryWaitMax
			}
			select {
			case <-time.After(backoff):
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
			backoff *= 2
			continue
		}
		break
	}
	return resp, err
}

func drainBody(body io.ReadCloser) {
	defer body.Close()
	_, err := io.Copy(io.Discard, io.LimitReader(body, respReadLimit))
	if err != nil {
		log.Warnf("error reading response body: %s", err.Error())
	}
}

func SetTokenCookie(token string, baseHRef string, isSecure bool, w http.ResponseWriter) error {
	var path string
	if baseHRef != "" {
		path = strings.TrimRight(strings.TrimLeft(baseHRef, "/"), "/")
	}
	cookiePath := "path=/" + path
	flags := []string{cookiePath, "SameSite=lax", "httpOnly"}
	if isSecure {
		flags = append(flags, "Secure")
	}
	cookies, err := MakeCookieMetadata(common.AuthCookieName, token, flags...)
	if err != nil {
		return fmt.Errorf("error creating cookie metadata: %w", err)
	}
	for _, cookie := range cookies {
		w.Header().Add("Set-Cookie", cookie)
	}
	return nil
}
