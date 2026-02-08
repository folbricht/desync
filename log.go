package desync

import (
	"io"

	"github.com/sirupsen/logrus"
)

var Log = logrus.New()

func init() {
	Log.SetOutput(io.Discard)
}
