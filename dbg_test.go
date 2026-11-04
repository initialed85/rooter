package main

import "testing"

func TestDbgFlush(t *testing.T) {
	a := newFake(t, "a", "test-model", "")
	e := newEnv(t, []string{"a=" + a.url()}, nil)
	res, body := e.do(postJSON("/v1/chat/completions", chatBody("s", "hi")))
	t.Logf("status=%d body=%q", res.StatusCode, body)
}
