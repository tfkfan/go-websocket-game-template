package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port              string
	WebSocketEndpoint string
	RoomEndpoint      string
	AssetsDir         string
	RoomTimeout       time.Duration
	PlayersBufferSize int
}

func Read() (*Config, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, errors.New("failed to load .env")
	}

	var config Config
	port, e := lookupEnv("PORT")
	if e != nil {
		return nil, e
	}
	config.Port = port

	webSocketEndpoint, e := lookupEnv("WS_ENDPOINT")
	if e != nil {
		return nil, e
	}
	config.WebSocketEndpoint = webSocketEndpoint

	assetsDir, e := lookupEnv("ASSETS_DIR")
	if e != nil {
		return nil, e
	}
	config.AssetsDir = assetsDir

	roomEndpoint, e := lookupEnv("ROOM_ENDPOINT")
	if e != nil {
		return nil, e
	}
	config.RoomEndpoint = roomEndpoint

	roomTimeoutRaw, e := lookupEnv("ROOM_TIMEOUT")
	if e != nil {
		return nil, e
	}
	roomTimeout, e := time.ParseDuration(roomTimeoutRaw)
	if e != nil {
		return nil, e
	}
	config.RoomTimeout = roomTimeout

	playersBufferSizeRaf, e := lookupEnv("PLAYERS_BUFFER_SIZE")
	if e != nil {
		return nil, e
	}
	playersBufferSize, e := strconv.Atoi(playersBufferSizeRaf)
	if e != nil {
		return nil, err
	}
	config.PlayersBufferSize = playersBufferSize

	return &config, nil
}

func lookupEnv(env string) (string, error) {
	value, exists := os.LookupEnv(env)
	if !exists {
		return "", fmt.Errorf("%s environment variable not set", env)
	}
	return value, nil
}
