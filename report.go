package rbforwarder

type report struct {
	seq     uint64
	code    int
	status  string
	retries int
}

func (r report) Status() (code int, status string, retries int) {
	return r.code, r.status, r.retries
}
