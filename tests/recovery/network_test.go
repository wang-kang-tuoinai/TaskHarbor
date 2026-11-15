package recovery

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type frame struct {
	raw   []byte
	text  string
	parts []frame
}

// 演练客户端固定 RESP2，保留原始字节，使迟到请求来自真实 Worker。
func readFrame(reader *bufio.Reader) (frame, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return frame{}, err
	}
	if len(line) < 3 || !strings.HasSuffix(line, "\r\n") {
		return frame{}, errors.New("invalid RESP line")
	}
	value := frame{raw: []byte(line), text: line[1 : len(line)-2]}
	switch line[0] {
	case '+', '-', ':':
		return value, nil
	case '$', '*':
		count, err := strconv.Atoi(value.text)
		if err != nil || count < -1 || count > 4<<20 {
			return frame{}, errors.New("invalid RESP length")
		}
		if count == -1 {
			return value, nil
		}
		if line[0] == '$' {
			body := make([]byte, count+2)
			if _, err := io.ReadFull(reader, body); err != nil {
				return frame{}, err
			}
			if string(body[count:]) != "\r\n" {
				return frame{}, errors.New("invalid RESP bulk terminator")
			}
			value.raw = append(value.raw, body...)
			value.text = string(body[:count])
			return value, nil
		}
		for range count {
			part, err := readFrame(reader)
			if err != nil {
				return frame{}, err
			}
			value.parts = append(value.parts, part)
			value.raw = append(value.raw, part.raw...)
		}
		return value, nil
	default:
		return frame{}, fmt.Errorf("unsupported RESP2 marker %q", line[0])
	}
}

type proxyConnection struct {
	client net.Conn
	redis  net.Conn
}

type heldCommand struct {
	TaskID string `json:"task_id"`
	Token  string `json:"token"`
	Wire   []byte `json:"wire_base64"`
	Reply  string `json:"reply,omitempty"`
}

type networkProxy struct {
	listener net.Listener
	upstream string
	emit     func(event)
	mu       sync.Mutex
	mode     string
	target   string
	armed    string
	resume   chan struct{}
	held     *heldCommand
	conns    map[*proxyConnection]bool
	stop     chan struct{}
	accepted chan struct{}
	workers  sync.WaitGroup
}

func newProxy(upstream string, emit func(event)) (*networkProxy, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &networkProxy{listener: listener, upstream: upstream, emit: emit, mode: "open",
		conns: make(map[*proxyConnection]bool), stop: make(chan struct{}), accepted: make(chan struct{})}
	go func() {
		defer close(p.accepted)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			pair := &proxyConnection{client: conn}
			p.mu.Lock()
			p.conns[pair] = true
			p.mu.Unlock()
			p.workers.Go(func() { p.serve(pair) })
		}
	}()
	return p, nil
}

func (p *networkProxy) Addr() string { return p.listener.Addr().String() }

func (p *networkProxy) serve(pair *proxyConnection) {
	defer func() {
		_ = pair.client.Close()
		if pair.redis != nil {
			_ = pair.redis.Close()
		}
		p.mu.Lock()
		delete(p.conns, pair)
		p.mu.Unlock()
	}()
	upstream, err := net.DialTimeout("tcp", p.upstream, time.Second)
	if err != nil {
		p.emit(event{Kind: "proxy_upstream_error", Detail: err.Error()})
		return
	}
	p.mu.Lock()
	pair.redis = upstream
	p.mu.Unlock()
	requests, responses := bufio.NewReader(pair.client), bufio.NewReader(upstream)
	for {
		request, err := readFrame(requests)
		if err != nil {
			return
		}
		if p.block(request) {
			return
		}
		if _, err := io.Copy(upstream, bytes.NewReader(request.raw)); err != nil {
			p.block(frame{}) // 黑洞时保留客户端连接，让真实 socket deadline 生效。
			return
		}
		response, err := readFrame(responses)
		if err != nil {
			p.block(frame{})
			return
		}
		if p.block(frame{}) {
			return
		}
		if _, err := io.Copy(pair.client, bytes.NewReader(response.raw)); err != nil {
			return
		}
	}
}

func (p *networkProxy) block(request frame) bool {
	p.mu.Lock()
	if len(request.parts) == 9 {
		args := request.parts
		// complete 的三 key 请求末尾为 SetResult 的 JSON，Renew 末尾为租约毫秒。
		if (args[0].text == "evalsha" || args[0].text == "eval") && args[2].text == "3" &&
			args[6].text == p.armed && p.mode == "open" && strings.HasPrefix(args[8].text, "{") {
			p.isolateLocked("blackhole", p.armed)
			p.held = &heldCommand{TaskID: p.target, Token: args[7].text, Wire: append([]byte(nil), request.raw...)}
			p.emit(event{Kind: "fault_started", TaskID: p.target, Detail: "blackhole at real result request"})
			p.emit(event{Kind: "old_request_held", TaskID: p.target, Detail: args[7].text})
			p.armed = ""
		}
	}
	mode, resume := p.mode, p.resume
	p.mu.Unlock()
	if mode == "open" {
		return false
	}
	if len(request.raw) != 0 {
		p.emit(event{Kind: "proxy_blocked", Detail: mode})
	}
	if mode == "blackhole" {
		select {
		case <-resume:
		case <-p.stop:
		}
	}
	return true
}

func (p *networkProxy) Isolate(mode, target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.isolateLocked(mode, target)
}

func (p *networkProxy) ArmLongOutage(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.armed = target
}

func (p *networkProxy) isolateLocked(mode, target string) {
	p.mode, p.target, p.resume = mode, target, make(chan struct{})
	for pair := range p.conns {
		if pair.redis != nil {
			_ = pair.redis.Close()
		}
		if mode == "cut" {
			_ = pair.client.Close()
		}
	}
}

func (p *networkProxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.mode != "open" {
		p.mode = "open"
		close(p.resume)
	}
}

func (p *networkProxy) DeliverHeld(ctx context.Context) (heldCommand, error) {
	p.mu.Lock()
	if p.held == nil {
		p.mu.Unlock()
		return heldCommand{}, errors.New("no real delayed report was captured")
	}
	held := *p.held
	p.mu.Unlock()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", p.upstream)
	if err != nil {
		return held, err
	}
	defer conn.Close()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(3 * time.Second)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return held, err
	}
	if _, err := io.Copy(conn, bytes.NewReader(held.Wire)); err != nil {
		return held, err
	}
	response, err := readFrame(bufio.NewReader(conn))
	if err != nil {
		return held, err
	}
	held.Reply = string(response.raw)
	if held.Reply != ":0\r\n" {
		return held, fmt.Errorf("old report was not explicitly rejected: %q", held.Reply)
	}
	return held, nil
}

func (p *networkProxy) Close() {
	close(p.stop)
	_ = p.listener.Close()
	<-p.accepted
	p.mu.Lock()
	for pair := range p.conns {
		_ = pair.client.Close()
		if pair.redis != nil {
			_ = pair.redis.Close()
		}
	}
	p.mu.Unlock()
	p.workers.Wait()
}
