package server

import (
	"context"

	"github.com/tfkfan/gogame/internal/game/player"
)

type OnJoin interface {
	OnJoin(ctx context.Context, p *player.Player)
}

type OnDisconnect interface {
	OnDisconnect(ctx context.Context, p *player.Player)
}
