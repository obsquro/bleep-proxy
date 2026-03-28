package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type ServerConfig struct {
	Port      string           `json:"port"`
	API       string           `json:"api"`
	Clients   []ClientAuth     `json:"clients"`
	Listeners []ListenerConfig `json:"listeners"`
}

type ClientAuth struct {
	ID  string `json:"id"`
	Key string `json:"key"`
}

type ListenerConfig struct {
	Client   string `json:"client"`
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
	Priority int    `json:"priority"`
}

type ClientConfig struct {
	ID     string `json:"id"`
	Key    string `json:"key"`
	Local  string `json:"local"`
	Server string `json:"server"`
}

func LoadServerConfig(path string) (*ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg ServerConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

func (c *ServerConfig) Validate() error {
	if c.Port == "" {
		return fmt.Errorf("port is required")
	}

	port, err := strconv.Atoi(c.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid port: %s", c.Port)
	}

	if len(c.Clients) == 0 {
		return fmt.Errorf("at least one client is required")
	}

	clientIDs := make(map[string]bool)
	for _, cl := range c.Clients {
		if cl.ID == "" {
			return fmt.Errorf("client id cannot be empty")
		}
		if cl.Key == "" {
			return fmt.Errorf("client key cannot be empty for %s", cl.ID)
		}
		if clientIDs[cl.ID] {
			return fmt.Errorf("duplicate client id: %s", cl.ID)
		}
		clientIDs[cl.ID] = true
	}

	if len(c.Listeners) == 0 {
		return fmt.Errorf("at least one listener is required")
	}

	ports := make(map[int]bool)
	for _, l := range c.Listeners {
		if l.Protocol != "tcp" && l.Protocol != "udp" {
			return fmt.Errorf("invalid protocol: %s (must be tcp or udp)", l.Protocol)
		}
		if l.Port < 1 || l.Port > 65535 {
			return fmt.Errorf("invalid port: %d", l.Port)
		}
		if l.Client == "" {
			return fmt.Errorf("client is required for listener on port %d", l.Port)
		}
		if !clientIDs[l.Client] {
			return fmt.Errorf("client %s not found in clients", l.Client)
		}

		key := l.Port
		if ports[key] {
			return fmt.Errorf("duplicate listener port: %d", l.Port)
		}
		ports[key] = true
	}

	return nil
}

func LoadClientConfig(path string) (*ClientConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg ClientConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

func (c *ClientConfig) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("id is required")
	}
	if c.Key == "" {
		return fmt.Errorf("key is required")
	}
	if c.Local == "" {
		return fmt.Errorf("local is required")
	}
	if c.Server == "" {
		return fmt.Errorf("server is required")
	}

	if !strings.Contains(c.Local, ":") {
		return fmt.Errorf("local must include port (e.g., 127.0.0.1:25565)")
	}

	if !strings.Contains(c.Server, ":") {
		return fmt.Errorf("server must include port (e.g., 155.212.147.144:7000)")
	}

	return nil
}
