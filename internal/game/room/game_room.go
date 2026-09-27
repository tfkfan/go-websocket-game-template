package room

import (
	"context"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/tfkfan/gogame/internal/game/player"
)

type GameRoom struct {
	Id uuid.UUID

	closed               bool
	gameRoomContext      context.Context
	gameRoomCancel       context.CancelFunc
	playersMu            sync.Mutex
	playersMessageBuffer int
	players              map[uuid.UUID]*player.Player
}

func NewGameRoom(gameRoomContext context.Context, gameRoomCancel context.CancelFunc, playersBuffer int) *GameRoom {
	r := &GameRoom{
		Id:                   uuid.New(),
		playersMessageBuffer: playersBuffer,
		players:              make(map[uuid.UUID]*player.Player),
		gameRoomContext:      gameRoomContext,
		gameRoomCancel:       gameRoomCancel,
	}

	return r
}

func (gr *GameRoom) Broadcast(msg []byte) {
	if gr.closed {
		return
	}

	gr.playersMu.Lock()
	defer gr.playersMu.Unlock()

	for _, subscriber := range gr.players {
		select {
		case subscriber.OutMessages <- msg:
		}
	}
}

func (gr *GameRoom) Send(recipient uuid.UUID, msg []byte) {
	if gr.closed {
		return
	}

	p, ok := gr.players[recipient]
	if !ok {
		return
	}

	gr.playersMu.Lock()
	defer gr.playersMu.Unlock()

	select {
	case p.OutMessages <- msg:
	}
}

func (gr *GameRoom) AddPlayer(p *player.Player) {
	if gr.closed {
		return
	}

	gr.playersMu.Lock()
	defer gr.playersMu.Unlock()

	gr.players[p.Id] = p
}

func (gr *GameRoom) DeletePlayer(p *player.Player) {
	if gr.closed {
		return
	}

	gr.playersMu.Lock()
	defer gr.playersMu.Unlock()

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
		msg, err := p.Read(gr.gameRoomContext)

		if err != nil {
			return err
		}

		select {
		case <-gr.gameRoomContext.Done():
			return gr.gameRoomContext.Err()
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
			p.Write(gr.gameRoomContext, msg)
		case <-gr.gameRoomContext.Done():
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
		case <-gr.gameRoomContext.Done():
			return
		}
	}
}

func (gr *GameRoom) Close(closeCallback func(gr *GameRoom)) {
	if gr.closed {
		return
	}

	gr.playersMu.Lock()
	defer gr.playersMu.Unlock()
	for _, p := range gr.players {
		close(p.InMessages)
		close(p.OutMessages)
	}
	gr.players = nil
	gr.gameRoomCancel()
	gr.closed = true

	closeCallback(gr)
}

func (gr *GameRoom) Run(ctx context.Context, closeCallback func(gr *GameRoom)) {
	timer := time.NewTimer(time.Minute)
	ticker := time.NewTicker(time.Second * 2)
	counter := 0
	for {
		select {
		case <-timer.C:
			gr.Close(closeCallback)
			log.Printf("Game room %s closed because of timeout", gr.Id)
			return
		case <-ctx.Done():
			timer.Stop()
			ticker.Stop()
			gr.Close(closeCallback)
			log.Printf("Game room %s closed because context cancel", gr.Id)
			return

		case <-ticker.C:
			counter++
			gr.Broadcast([]byte("hello from server #" + strconv.Itoa(counter)))

			log.Printf("Game room %s contains %d players", gr.Id, len(gr.players))
		}
	}
}
