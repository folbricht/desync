package desync

type ProgressBar interface {
	Add(n int)
	Set(n int)
	Start()
	Stop()
}
