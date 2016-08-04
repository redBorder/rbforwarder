package rbforwarder

type report struct {
	code    int
	status  string
	retries int
	opts    map[string]interface{}
}

func (r report) Status() (code int, status string, retries int) {
	return r.code, r.status, r.retries
}

func (r report) GetOpts() map[string]interface{} {
	return r.opts
}
