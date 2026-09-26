package player

import (
	"context"

	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type Player struct {
	Id          uuid.UUID
	ws          *websocket.Conn
	InMessages  chan []byte
	OutMessages chan []byte
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
