package room

import (
	"context"

	"github.com/tfkfan/gogame/internal/server"
)

type GameRoom struct {
	ws *server.GameServer
}

func NewGameRoom(ws *server.GameServer) *GameRoom {
	r := &GameRoom{}
	r.ws = ws
	return r
}

func (r *GameRoom) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		default:
			//tm.UpdateState()
			//ws.publish([]byte(fmt.Sprintf("Temperature: %f", tm.GetCurrent())))
		}
	}
}
