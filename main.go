package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tfkfan/gogame/internal/game/room"
	"github.com/tfkfan/gogame/internal/server"
)

func main() {
	log.SetFlags(0)

	err := run()
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return errors.New("please provide an address to listen on as the first argument")
	}

	port := os.Args[1]

	srv, errc, e := server.NewGameServer(port, "./assets", "/ws")
	if e != nil {
		return e
	}

	gCtx, gCancel := context.WithCancel(context.Background())
	defer gCancel()

	gameRoom := room.NewGameRoom(srv)
	go gameRoom.Run(gCtx)

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errc:
		log.Printf("failed to serve: %v", err)
	case sig := <-sigs:
		log.Printf("terminating: %v", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	return srv.HttpServer.Shutdown(ctx)
}
