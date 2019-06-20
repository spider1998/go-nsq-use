package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/getsentry/raven-go"
	"github.com/inconshreveable/log15"
)

type Logger interface {
	log15.Logger
}

func New(debug bool, module string) (Logger, error) {
	lvl := log15.LvlInfo
	if debug {
		lvl = log15.LvlDebug
	}
	logger := log15.Root()
	h := log15.LvlFilterHandler(lvl, log15.CallerFileHandler(logger.GetHandler()))
	if sentryDSN := os.Getenv("SENTRY_DSN"); sentryDSN != "" {
		sentryClient, err := raven.New(sentryDSN)
		if err != nil {
			return nil, err
		}
		h = log15.MultiHandler(h, log15.LvlFilterHandler(log15.LvlWarn, log15.FuncHandler(func(r *log15.Record) error {
			tags := make(map[string]string, len(r.Ctx)/2)
			tags["level"] = r.Lvl.String()
			tags["module"] = module
			buf := &bytes.Buffer{}
			for i := 0; i < len(r.Ctx); i += 2 {
				if key, ok := r.Ctx[i].(string); ok {
					if value, ok := format(r.Ctx[i+1]); ok {
						tags[key] = value
						continue
					}
				}
				buf.WriteString(fmt.Sprintf(" %v=%+v", r.Ctx[i], r.Ctx[i+1]))
			}
			message := r.Msg
			if buf.Len() > 0 {
				message += " (" + strings.TrimPrefix(buf.String(), " ") + ")"
			}
			sentryClient.CaptureError(Message(message), tags)
			return nil
		})))
	}
	logger.SetHandler(h)
	return logger, nil
}

func format(a interface{}) (v string, ok bool) {
	switch a.(type) {
	case int, int8, int16, int32, int64, float32, float64, uint, uint8, uint16, uint32, uint64, string:
		v = fmt.Sprint(a)
		return v, len(v) < 32
	}
	return
}

type Message string

func (e Message) Error() string {
	return string(e)
}

func NewNSQLogger(logger Logger) NSQLogger {
	return NSQLogger{logger}
}

type NSQLogger struct {
	logger Logger
}

func (logger NSQLogger) Output(calldepth int, s string) error {
	logger.logger.Info(s, "service", "nsq_logger")
	return nil
}
