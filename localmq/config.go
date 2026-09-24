package localmq

import (
	"log/slog"
	"time"
)

// Config holds broker configuration. All zero values are replaced by
// defaults via DefaultConfig / normalize.
type Config struct {
	// Dir is the directory used for the append-only write-ahead log.
	// It is created if missing.
	Dir string

	// MaxAttempts is the maximum number of delivery attempts for a message
	// within a consumer group. When the last attempt ends with a temporary
	// failure the message is moved to the dead-letter queue. Must be >= 1.
	MaxAttempts int

	// RetryBaseDelay / RetryMaxDelay configure exponential backoff for
	// temporary failures: delay(attempt n) = min(BaseDelay * 2^(n-1), MaxDelay).
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	// ScheduleTick is how often the scheduler scans for ready messages and
	// available retry slots.
	ScheduleTick time.Duration

	// Fsync forces an fsync after every persisted record. When false records
	// may be lost on OS/host crash (process crash is still safe).
	Fsync bool

	// CloseTimeout bounds graceful shutdown: in-flight handlers get this long
	// to finish before the context passed to them is canceled.
	CloseTimeout time.Duration

	// Logger is used for structured operational logs.
	Logger *slog.Logger
}

// DefaultConfig returns a Config with sane defaults rooted at dir.
func DefaultConfig(dir string) Config {
	return Config{
		Dir:            dir,
		MaxAttempts:    5,
		RetryBaseDelay: 10 * time.Millisecond,
		RetryMaxDelay:  1 * time.Second,
		ScheduleTick:   5 * time.Millisecond,
		Fsync:          true,
		CloseTimeout:   10 * time.Second,
		Logger:         slog.Default(),
	}
}

func (c *Config) normalize() {
	def := DefaultConfig(c.Dir)
	if c.MaxAttempts < 1 {
		c.MaxAttempts = def.MaxAttempts
	}
	if c.RetryBaseDelay <= 0 {
		c.RetryBaseDelay = def.RetryBaseDelay
	}
	if c.RetryMaxDelay <= 0 {
		c.RetryMaxDelay = def.RetryMaxDelay
	}
	if c.RetryMaxDelay < c.RetryBaseDelay {
		c.RetryMaxDelay = c.RetryBaseDelay
	}
	if c.ScheduleTick <= 0 {
		c.ScheduleTick = def.ScheduleTick
	}
	if c.CloseTimeout <= 0 {
		c.CloseTimeout = def.CloseTimeout
	}
	if c.Logger == nil {
		c.Logger = def.Logger
	}
}
