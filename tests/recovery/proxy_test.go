package recovery

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"
)

func TestProxyIsolatesExistingAndNewConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					if _, err := readFrame(reader); err != nil {
						return
					}
					if _, err := io.WriteString(conn, "+PONG\r\n"); err != nil {
						return
					}
				}
			}()
		}
	}()
	proxy, err := newProxy(listener.Addr().String(), func(event) {})
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	connect := func() net.Conn {
		t.Helper()
		conn, err := net.DialTimeout("tcp", proxy.Addr(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}
	ping := func(conn net.Conn) error {
		if err := conn.SetDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			return err
		}
		if _, err := io.WriteString(conn, "*1\r\n$4\r\nPING\r\n"); err != nil {
			return err
		}
		_, err := readFrame(bufio.NewReader(conn))
		return err
	}
	existing := connect()
	if err := ping(existing); err != nil {
		t.Fatal(err)
	}
	proxy.Isolate("blackhole", "")
	for _, conn := range []net.Conn{existing, connect()} {
		began := time.Now()
		err := ping(conn)
		timeout, ok := err.(net.Error)
		if !ok || !timeout.Timeout() || time.Since(began) < 70*time.Millisecond {
			t.Fatalf("blackhole did not cause a real socket timeout: %v", err)
		}
	}
	proxy.Restore()
	if err := ping(connect()); err != nil {
		t.Fatal(err)
	}
	proxy.Isolate("cut", "")
	if err := ping(connect()); err == nil {
		t.Fatal("a new connection bypassed cut isolation")
	}
	proxy.Restore()
	if err := ping(connect()); err != nil {
		t.Fatal(err)
	}
	if _, err := proxy.DeliverHeld(context.Background()); err == nil {
		t.Fatal("missing delayed command was accepted as evidence")
	}
}

func TestProxyDelaysAnActualWorkerReportUntilExplicitDelivery(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	delivered := make(chan []byte, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				command, err := readFrame(bufio.NewReader(conn))
				if err == nil {
					delivered <- command.raw
					_, _ = io.WriteString(conn, ":0\r\n")
				}
			}()
		}
	}()
	events := make(chan event, 8)
	proxy, err := newProxy(listener.Addr().String(), func(e event) { events <- e })
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	proxy.ArmLongOutage("target")
	conn, err := net.Dial("tcp", proxy.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// 与真实 complete 一样的 RESP2 形状，只在完整演练中由平台生成实际命令。
	wire := []byte("*10\r\n$7\r\nevalsha\r\n$3\r\nsha\r\n$1\r\n3\r\n$17\r\ntaskq:meta:target\r\n$6\r\nactive\r\n$5\r\nlease\r\n$6\r\ntarget\r\n$5\r\ntoken\r\n$12\r\n{\"square\":9}\r\n$7\r\ndefault\r\n")
	if _, err := conn.Write(wire); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-events:
		if e.Kind != "fault_started" {
			t.Fatalf("unexpected event: %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("actual report did not trigger isolation")
	}
	select {
	case <-delivered:
		t.Fatal("report reached Redis before release")
	default:
	}
	proxy.Restore()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	held, err := proxy.DeliverHeld(ctx)
	if err != nil || held.Token != "token" || held.Reply != ":0\r\n" || !bytes.Equal(held.Wire, wire) {
		t.Fatalf("held=%+v err=%v", held, err)
	}
	if got := <-delivered; !bytes.Equal(got, wire) {
		t.Fatal("delivery changed the original command")
	}
}
