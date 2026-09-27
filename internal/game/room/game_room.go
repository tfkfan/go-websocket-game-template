package room

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tfkfan/gogame/internal/config"
	"github.com/tfkfan/gogame/internal/game/player"
)

type GameRoom struct {
	Id uuid.UUID

	closed bool

	ctx         context.Context
	ctxCancel   context.CancelFunc
	roomTimeout time.Duration

	roomMu               sync.Mutex
	playersMessageBuffer int
	players              map[uuid.UUID]*player.Player
}

func NewGameRoom(gameRoomContext context.Context, gameRoomCancel context.CancelFunc, srvCfg *config.Config) *GameRoom {
	r := &GameRoom{
		Id:                   uuid.New(),
		playersMessageBuffer: 16,
		players:              make(map[uuid.UUID]*player.Player),
		ctx:                  gameRoomContext,
		ctxCancel:            gameRoomCancel,
		roomTimeout:          srvCfg.RoomTimeout,
	}

	return r
}

func (gr *GameRoom) Broadcast(msg []byte) {
	gr.roomMu.Lock()
	defer gr.roomMu.Unlock()

	if gr.closed {
		return
	}

	for _, subscriber := range gr.players {
		select {
		case subscriber.OutMessages <- msg:
		}
	}
}

func (gr *GameRoom) Send(recipient uuid.UUID, msg []byte) {
	p, ok := gr.players[recipient]
	if !ok {
		return
	}

	gr.roomMu.Lock()
	defer gr.roomMu.Unlock()

	if gr.closed {
		return
	}

	select {
	case p.OutMessages <- msg:
	}
}

func (gr *GameRoom) AddPlayer(p *player.Player) {
	gr.roomMu.Lock()
	defer gr.roomMu.Unlock()

	if gr.closed {
		return
	}

	gr.players[p.Id] = p
}

func (gr *GameRoom) DeletePlayer(p *player.Player) {
	gr.roomMu.Lock()
	defer gr.roomMu.Unlock()

	if gr.closed {
		return
	}

	delete(gr.players, p.Id)
	close(p.InMessages)
	close(p.OutMessages)
}

func (gr *GameRoom) OnJoin(p *player.Player) {
	log.Printf("player %s joined", p.Id)

	go gr.ListenIn(p)
	go gr.ListenOut(p)

	gr.AddPlayer(p)
}

func (gr *GameRoom) OnDisconnect(p *player.Player) {
	log.Printf("player %s disconnected", p.Id)

	gr.DeletePlayer(p)
}

func (gr *GameRoom) ReadWebSocket(p *player.Player) error {
	for {
		msg, err := p.Read(gr.ctx)

		if err != nil {
			return err
		}

		select {
		case <-gr.ctx.Done():
			return gr.ctx.Err()
		case p.InMessages <- msg:
		}
	}
}

func (gr *GameRoom) ListenOut(p *player.Player) {
	for {
		select {
		case msg, ok := <-p.OutMessages:
			if !ok {
				return
			}
			p.Write(gr.ctx, msg)
		case <-gr.ctx.Done():
			return
		}
	}
}

func (gr *GameRoom) ListenIn(p *player.Player) {
	for {
		select {
		case msg, ok := <-p.InMessages:
			if !ok {
				return
			}
			log.Printf("received incoming message: %s", msg)
		case <-gr.ctx.Done():
			return
		}
	}
}

func (gr *GameRoom) Close(closeCallback func(gr *GameRoom)) {
	if gr.closed {
		return
	}

	gr.roomMu.Lock()
	defer gr.roomMu.Unlock()

	for _, p := range gr.players {
		close(p.InMessages)
		close(p.OutMessages)
	}

	gr.players = nil
	gr.ctxCancel()
	gr.closed = true

	closeCallback(gr)
}

func (gr *GameRoom) Run(ctx context.Context, closeCallback func(gr *GameRoom)) {
	timer := time.NewTimer(gr.roomTimeout)
	ticker := time.NewTicker(time.Second * 2)
	counter := 0

	stop := func() {
		timer.Stop()
		ticker.Stop()
		gr.Close(closeCallback)
	}

	for {
		select {
		case <-timer.C:
			stop()
			log.Printf("Game room %s closed because of timeout", gr.Id)
			return
		case <-ctx.Done():
			stop()
			log.Printf("Game room %s closed because context cancel", gr.Id)
			return
			//Main game loop should be with default keyword only. Ticker here as imitation of game loop tick
		case <-ticker.C:
			counter++
			gr.Broadcast([]byte("hello from server #" + strconv.Itoa(counter)))

			log.Printf("Game room %s contains %d players", gr.Id, len(gr.players))
		}
	}
}
