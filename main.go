package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

const version = "0.1.0"

type Config struct {
	Listen         string
	BackendSpecs   []string
	backends       []*Backend
	SessionTTL     time.Duration
	MaxSessions    int
	MaxWait        time.Duration
	StickyGrace    time.Duration
	Retries        int
	RetryWait      time.Duration
	HealthInterval time.Duration
	HealthTimeout  time.Duration
	RequestTimeout time.Duration
	ScanWindow     int
	MaxBody        int64
	FreePaths      []string
	XForwarded     bool
	Verbose        bool
	Quiet          bool
	Version        bool
}

// defaultFreePaths never occupy a concurrency slot (matched as a suffix).
var defaultFreePaths = []string{
	"/models", "/health", "/healthz", "/props", "/tokenize", "/detokenize",
	"/apply-template", "/version", "/metrics",
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(v string) error {
	for _, piece := range strings.Split(v, "\n") {
		if t := strings.TrimSpace(piece); t != "" {
			*s = append(*s, t)
		}
	}
	return nil
}

func parseFlags(args []string) (*Config, error) {
	c := &Config{}
	fs := flag.NewFlagSet("rooter", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `rooter %s - a transparent local LLM router

Usage:
  rooter [flags] -backend NAME=URL[,token=TOKEN][,concurrency=N] -backend ...

Flags:
`, version)
		fs.PrintDefaults()
		fmt.Fprint(os.Stderr, `
Backend spec keys: name, url (or base), token (or api-key), concurrency (or slots).
Shorthand: -backend 'strata=http://192.168.11.128:8088/v1,token=Password1,concurrency=4'
`)
	}
	fs.StringVar(&c.Listen, "listen", ":8089", "address for rooter to listen on")
	fs.Var((*stringList)(&c.BackendSpecs), "backend", "backend spec, repeat once per backend")
	fs.DurationVar(&c.SessionTTL, "session-ttl", 6*time.Hour, "forget a session after this long idle (0 = never)")
	fs.IntVar(&c.MaxSessions, "max-sessions", 20000, "cap on tracked sessions (LRU evicted)")
	fs.DurationVar(&c.MaxWait, "max-wait", 0, "how long to block for a free slot before 503 (0 = block forever)")
	fs.DurationVar(&c.StickyGrace, "sticky-grace", 20*time.Second, "keep waiting on an unusable sticky backend this long before rebinding")
	fs.IntVar(&c.Retries, "retries", 1, "extra attempts on other backends when one dies before responding (0 = never)")
	fs.DurationVar(&c.RetryWait, "retry-wait", 5*time.Second, "max wait for an alternative backend when retrying")
	fs.DurationVar(&c.HealthInterval, "health-interval", 15*time.Second, "how often to poll backend /models (0 = only at start)")
	fs.DurationVar(&c.HealthTimeout, "health-timeout", 10*time.Second, "per-backend health check timeout")
	fs.DurationVar(&c.RequestTimeout, "request-timeout", 0, "hard cap on a generation request, queue included (0 = none)")
	fs.IntVar(&c.ScanWindow, "scan-window", 1<<20, "leading body bytes always read to classify the session")
	fs.Int64Var(&c.MaxBody, "max-body", 64<<20, "buffer bodies up to this many bytes so they can be replayed")
	fs.Var((*stringList)(&c.FreePaths), "free-path", "path suffix that never needs a slot (repeatable)")
	fs.BoolVar(&c.XForwarded, "xff", false, "let X-Forwarded-For be added to upstream requests")
	fs.BoolVar(&c.Verbose, "verbose", false, "log retries, queue detail and more")
	fs.BoolVar(&c.Quiet, "quiet", false, "only log requests that failed")
	fs.BoolVar(&c.Version, "version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if c.Version {
		return c, nil
	}
	if len(c.FreePaths) == 0 {
		c.FreePaths = defaultFreePaths
	}
	if c.Version {
		return c, nil
	}
	if len(c.BackendSpecs) == 0 {
		fs.Usage()
		return nil, fmt.Errorf("no -backend given")
	}
	return c, nil
}

func main() {
	args := os.Args[1:]
	cfg, err := parseFlags(args)
	if err != nil {
		if err == flag.ErrHelp {
			return
		}
		log.Fatalf("rooter: %v", err)
	}
	if cfg.Version {
		fmt.Printf("rooter %s\n", version)
		return
	}
	backends, err := parseBackends(cfg.BackendSpecs, 1)
	if err != nil {
		log.Fatalf("rooter: %v", err)
	}
	cfg.backends = backends

	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmsgprefix)
	logger.SetPrefix("rooter ")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool := NewPool(backends, PoolOpts{
		Poll:           time.Second,
		HealthInterval: cfg.HealthInterval,
		HealthTimeout:  cfg.HealthTimeout,
		Verbose:        cfg.Verbose,
	}, &http.Client{Timeout: cfg.HealthTimeout}, logger)
	pool.CheckAll()
	go pool.HealthLoop(ctx)

	sessions := NewSessionStore(cfg.SessionTTL, cfg.MaxSessions, logger)
	go sessions.Janitor(ctx, time.Minute)

	router := NewRouter(pool, sessions, cfg, logger)

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		log.Fatalf("rooter: listen %s: %v", cfg.Listen, err)
	}
	srv := &http.Server{
		Handler:           router,
		ReadHeaderTimeout: 60 * time.Second,
		IdleTimeout:       5 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}
	go func() {
		<-ctx.Done()
		logger.Printf("shutting down (in-flight requests are not drained)")
		srv.Close()
	}()
	names := make([]string, 0, len(backends))
	for _, b := range backends {
		names = append(names, fmt.Sprintf("%s(%d)", b.Name, b.Concurrency))
	}
	logger.Printf("listening on %s -> %s", ln.Addr(), strings.Join(names, " "))
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatalf("rooter: serve: %v", err)
	}
}
