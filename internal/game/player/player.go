package player

import (
	"context"
	"log"
	"net/http"
	"sync"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/tfkfan/gogame/internal/network"
)

type Player struct {
	Id          uuid.UUID
	ws          *websocket.Conn
	Mu          sync.RWMutex
	InMessages  chan []byte
	OutMessages chan []byte

	LastAction string
}

func ExtractPlayerFromRequest(r *http.Request, ws *websocket.Conn) (*Player, error) {
	pId, e := uuid.Parse(r.Header.Get("X-Player-Id"))
	if e != nil {
		pId = uuid.New()
	}
	p := NewPlayer(pId, ws, 16)
	return p, nil
}

func NewPlayer(id uuid.UUID, ws *websocket.Conn, playersBuffer int) *Player {
	return &Player{
		Id:          id,
		ws:          ws,
		InMessages:  make(chan []byte, playersBuffer),
		OutMessages: make(chan []byte, playersBuffer),
	}
}

func (p *Player) Read(ctx context.Context) ([]byte, error) {
	_, msg, err := p.ws.Read(ctx)
	if err != nil {
		return []byte{}, err
	}
	return msg, nil
}

func (p *Player) Write(ctx context.Context, msg []byte) {
	err := p.ws.Write(ctx, websocket.MessageText, msg)
	if err != nil {
		return
	}
}

func (p *Player) Send(msg []byte) {
	select {
	case p.OutMessages <- msg:
	}
}

func (p *Player) CloseWebSocket(statusCode websocket.StatusCode, errorMessage string) {
	network.CloseWebSocket(p.ws, statusCode, errorMessage)
}

func (p *Player) DoAction(action string) {
	log.Printf("action '%s' performed for player %s\n", action, p.Id)
	p.LastAction = action
}
