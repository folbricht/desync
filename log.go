package desync

import (
	"io/ioutil"

	"github.com/sirupsen/logrus"
)

var Log = logrus.New()

func init() {
	Log.SetOutput(ioutil.Discard)
}
