package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Port              string
	WebSocketEndpoint string
	RoomEndpoint      string
	AssetsDir         string
	RoomTimeout       time.Duration
}

func Read() (*Config, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, errors.New("failed to load .env")
	}

	var config Config
	port, e := lookupEnv("PORT")
	config.Port = port
	if e != nil {
		return nil, e
	}

	webSocketEndpoint, e := lookupEnv("WS_ENDPOINT")
	config.WebSocketEndpoint = webSocketEndpoint
	if e != nil {
		return nil, e
	}

	assetsDir, e := lookupEnv("ASSETS_DIR")
	config.AssetsDir = assetsDir
	if e != nil {
		return nil, e
	}

	roomEndpoint, e := lookupEnv("ROOM_ENDPOINT")
	config.RoomEndpoint = roomEndpoint
	if e != nil {
		return nil, e
	}

	roomTimeoutRaw, e := lookupEnv("ROOM_TIMEOUT")
	if e != nil {
		return nil, e
	}
	roomTimeout, e := time.ParseDuration(roomTimeoutRaw)

	config.RoomTimeout = roomTimeout
	if e != nil {
		return nil, e
	}

	return &config, nil
}

func lookupEnv(env string) (string, error) {
	value, exists := os.LookupEnv(env)
	if !exists {
		return "", fmt.Errorf("%s environment variable not set", env)
	}
	return value, nil
}
