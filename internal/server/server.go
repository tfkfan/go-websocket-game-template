package server

import (
	"context"
	"errors"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type GameServer struct {
	HttpServer              *http.Server
	subscriberMessageBuffer int

	serveMux http.ServeMux

	subscribersMu sync.Mutex
	subscribers   map[*subscriber]struct{}
}

func NewGameServer(port string, staticContentDir string, webSocketEndpoint string) (*GameServer, chan error, error) {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("listening on ws://%v", listener.Addr())

	srv := &GameServer{
		subscriberMessageBuffer: 16,
		subscribers:             make(map[*subscriber]struct{}),
	}
	srv.serveMux.Handle("/", http.FileServer(http.Dir(staticContentDir)))
	srv.serveMux.HandleFunc(webSocketEndpoint, srv.subscribeHandler)

	httpSrv := &http.Server{
		Handler:      srv,
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}
	srv.HttpServer = httpSrv

	errc := make(chan error, 1)
	go func() {
		errc <- httpSrv.Serve(listener)
	}()
	return srv, errc, nil
}

type subscriber struct {
	messages  chan []byte
	closeSlow func()
}

func (srv *GameServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	srv.serveMux.ServeHTTP(w, r)
}

func (srv *GameServer) subscribeHandler(w http.ResponseWriter, r *http.Request) {
	err := srv.subscribe(w, r)
	if errors.Is(err, context.Canceled) {
		return
	}
	if websocket.CloseStatus(err) == websocket.StatusNormalClosure ||
		websocket.CloseStatus(err) == websocket.StatusGoingAway {
		return
	}
	if err != nil {
		return
	}
}

func (srv *GameServer) subscribe(w http.ResponseWriter, r *http.Request) error {
	var mu sync.Mutex
	var c *websocket.Conn
	var closed bool
	s := &subscriber{
		messages: make(chan []byte, srv.subscriberMessageBuffer),
		closeSlow: func() {
			mu.Lock()
			defer mu.Unlock()
			closed = true
			if c != nil {
				c.Close(websocket.StatusPolicyViolation, "connection too slow to keep up with messages")
			}
		},
	}
	srv.addSubscriber(s)
	defer srv.deleteSubscriber(s)

	c2, err := websocket.Accept(w, r, nil)
	if err != nil {
		return err
	}
	mu.Lock()
	if closed {
		mu.Unlock()
		return net.ErrClosed
	}
	c = c2
	mu.Unlock()
	defer c.CloseNow()

	ctx := c.CloseRead(context.Background())

	for {
		select {
		case msg := <-s.messages:
			err := writeTimeout(ctx, time.Second*5, c, msg)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (srv *GameServer) publish(msg []byte) {
	srv.subscribersMu.Lock()
	defer srv.subscribersMu.Unlock()

	for s := range srv.subscribers {
		select {
		case s.messages <- msg:
		default:
			go s.closeSlow()
		}
	}
}

func (srv *GameServer) addSubscriber(s *subscriber) {
	srv.subscribersMu.Lock()
	srv.subscribers[s] = struct{}{}
	srv.subscribersMu.Unlock()
}

func (srv *GameServer) deleteSubscriber(s *subscriber) {
	srv.subscribersMu.Lock()
	delete(srv.subscribers, s)
	srv.subscribersMu.Unlock()
}

func writeTimeout(ctx context.Context, timeout time.Duration, c *websocket.Conn, msg []byte) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	return c.Write(ctx, websocket.MessageText, msg)
}
