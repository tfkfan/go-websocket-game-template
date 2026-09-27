package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/google/uuid"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestLeaks(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("App failed: %v", err)
	}
}

func TestLeaksWithRoom(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go func() {
		if err := run(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("App failed: %v", err)
		}
	}()

	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	roomId, e := createGameRoom(client)
	if e != nil {
		t.Errorf("Failed to create game room: %v", e)
	}

	dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	conn, resp, err := websocket.Dial(dialCtx, "ws://localhost:8080/ws?room="+roomId.String(), nil)
	cancel()
	if err != nil {
		t.Errorf("Dial failed: %v (reply: %v)", err, resp)
	}
	log.Println("Connection established")

	defer closeWebSocket(conn, websocket.StatusNormalClosure, "Client disconnected")
	readErrChan := make(chan error, 1)

	go clientReader(ctx, conn, readErrChan)
	clientWriter(ctx, conn, readErrChan)
}

func clientWriter(ctx context.Context, conn *websocket.Conn, readErrChan chan error) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("ctx cancelled...")
			return

		case err := <-readErrChan:
			log.Printf("shutdown with err: %v", err)
			return

		case <-ticker.C:
			msg := "hi from client"

			writeCtx, writeCancel := context.WithTimeout(ctx, 2*time.Second)
			err := wsjson.Write(writeCtx, conn, msg)
			writeCancel()
			if err != nil {
				log.Printf("send err: %v", err)
				return
			}
			log.Println("json successfully sent")
		}
	}
}

func clientReader(ctx context.Context, ws *websocket.Conn, readErrChan chan error) {
	for {
		msgType, data, err := ws.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			readErrChan <- fmt.Errorf("read err: %w", err)
			return
		}

		log.Printf("accepted [frame type %v]: %s", msgType, string(data))
	}
}

func createGameRoom(client *http.Client) (uuid.UUID, error) {
	req, err := http.NewRequest(http.MethodPost, "http://localhost:8080/room", bytes.NewBuffer([]byte{}))
	if err != nil {
		return uuid.UUID{}, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return uuid.UUID{}, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			return
		}
	}(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return uuid.UUID{}, err
	}

	return uuid.Parse(string(body))
}

func closeWebSocket(conn *websocket.Conn, code websocket.StatusCode, reason string) {
	err := conn.Close(code, reason)
	if err != nil {
		return
	}
}
