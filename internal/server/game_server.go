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
	"github.com/google/uuid"
	"github.com/tfkfan/gogame/internal/config"
	"github.com/tfkfan/gogame/internal/game/player"
	"github.com/tfkfan/gogame/internal/game/room"
	"github.com/tfkfan/gogame/internal/network"
)

type GameServer struct {
	HttpServer *http.Server

	serveMux     http.ServeMux
	serverCtx    context.Context
	serverCancel context.CancelFunc
	gameServerMu sync.Mutex

	cfg *config.Config

	rooms map[uuid.UUID]*room.GameRoom
}

func NewGameServer(cfg *config.Config) (*GameServer, chan error, error) {
	listener, err := net.Listen("tcp", cfg.Port)
	if err != nil {
		return nil, nil, err
	}
	log.Printf("listening on http://%v", listener.Addr())

	ctx, cancel := context.WithCancel(context.Background())
	srv := &GameServer{
		rooms:        make(map[uuid.UUID]*room.GameRoom),
		serverCtx:    ctx,
		serverCancel: cancel,
		cfg:          cfg,
	}
	srv.serveMux.Handle("/", http.FileServer(http.Dir(cfg.AssetsDir)))
	srv.serveMux.HandleFunc(cfg.WebSocketEndpoint, srv.handleWsConnection)
	srv.serveMux.HandleFunc(cfg.RoomEndpoint, srv.handleRoom)

	log.Printf("listening websocket on ws://%v%v", listener.Addr(), cfg.WebSocketEndpoint)

	httpSrv := &http.Server{
		Handler:      srv,
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}
	srv.HttpServer = httpSrv

	errChannel := make(chan error, 1)
	go func() {
		errChannel <- httpSrv.Serve(listener)
	}()
	return srv, errChannel, nil
}

func (srv *GameServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	srv.serveMux.ServeHTTP(w, r)
}

func (srv *GameServer) Shutdown(ctx context.Context) error {
	srv.serverCancel()
	return srv.HttpServer.Shutdown(ctx)
}

func (srv *GameServer) handleWsConnection(w http.ResponseWriter, r *http.Request) {
	ws, wsErr := websocket.Accept(w, r, nil)
	if wsErr == nil {
		wsErr = srv.tryConnect(r, ws)
	}
	if errors.Is(wsErr, context.Canceled) {
		return
	}
	if websocket.CloseStatus(wsErr) == websocket.StatusNormalClosure ||
		websocket.CloseStatus(wsErr) == websocket.StatusGoingAway {
		return
	}
	if wsErr != nil {
		log.Printf("error on websocket join: %v", wsErr)
		network.CloseWebSocket(ws, websocket.StatusBadGateway, wsErr.Error())
		return
	}
}

func (srv *GameServer) tryConnect(r *http.Request, ws *websocket.Conn) error {
	gr, e := room.ExtractGameRoomFromRequest(r, srv.rooms)
	if e != nil {
		return e
	}
	p, e := player.ExtractPlayerFromRequest(r, ws)
	if e != nil {
		return e
	}

	defer gr.OnDisconnect(p)

	if gr.AddPlayer(p) {
		gr.OnJoin(p)
		return gr.ReadWebSocket(p)
	}

	return errors.New("game room is full")
}

func (srv *GameServer) handleRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	gameRoomContext, gameRoomCancel := context.WithCancel(srv.serverCtx)

	gr := room.NewGameRoom(gameRoomContext, gameRoomCancel, srv.cfg)

	srv.gameServerMu.Lock()
	defer srv.gameServerMu.Unlock()

	srv.rooms[gr.Id] = gr

	go gr.Run(srv.serverCtx, func(room *room.GameRoom) {
		srv.DeleteGameRoom(room.Id)
	})

	txt, e := gr.Id.MarshalText()
	if e != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	_, err := w.Write(txt)
	if err != nil {
		return
	}
}

func (srv *GameServer) DeleteGameRoom(id uuid.UUID) {
	srv.gameServerMu.Lock()
	defer srv.gameServerMu.Unlock()
	delete(srv.rooms, id)
}
