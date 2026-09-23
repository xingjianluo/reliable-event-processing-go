package mq

import (
	"fmt"
	"log"
	"os"
)

type stdLogger struct {
	l *log.Logger
}

func newDefaultLogger() Logger {
	return &stdLogger{l: log.New(os.Stdout, "[mq] ", log.LstdFlags|log.Lmicroseconds)}
}

func (s *stdLogger) Printf(format string, args ...any) {
	s.l.Output(2, fmt.Sprintf(format, args...))
}
