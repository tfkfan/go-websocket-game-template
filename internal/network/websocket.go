package network

import "github.com/coder/websocket"

func CloseWebSocket(ws *websocket.Conn, statusCode websocket.StatusCode, statusMessage string) {
	e := ws.Close(statusCode, statusMessage)
	if e != nil {
		return
	}
}
