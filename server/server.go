package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// upgrades HTTP to WebSocket
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins
	},
}

// Client represents a WebSocket connection
type Client struct {
	Conn *websocket.Conn
	Send chan []byte
}

// Hub manages all WebSocket clients
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.Mutex
}

// NewHub initializes a new WebSocket hub
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run handles incoming connections, disconnections, and message broadcasting
func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
			log.Println("New client connected")

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.Send)
				client.Conn.Close()
				log.Println("Client disconnected")
			}
			h.mu.Unlock()

		case message := <-h.broadcast:
			h.mu.Lock()
			for client := range h.clients {
				select {
				case client.Send <- message:
				default:
					close(client.Send)
					delete(h.clients, client)
				}
			}
			h.mu.Unlock()
		}
	}
}

// HandleWebSocketConnection upgrades HTTP to WebSocket and manages the client
func HandleWebSocketConnection(hub *Hub, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket Upgrade Error:", err)
		return
	}

	client := &Client{
		Conn: conn,
		Send: make(chan []byte, 256),
	}

	hub.register <- client

	go client.ReadMessages(hub)
	go client.WriteMessages()
}

// ReadMessages listens for messages from the WebSocket client
func (c *Client) ReadMessages(hub *Hub) {
	defer func() {
		hub.unregister <- c
	}()
	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}
		log.Println("Received message:", string(message))
		hub.broadcast <- message
	}
}

// WriteMessages sends messages to the WebSocket client
func (c *Client) WriteMessages() {
	for message := range c.Send {
		err := c.Conn.WriteMessage(websocket.TextMessage, []byte("you told server: "+string(message)))
		if err != nil {
			break
		}
	}
}

func main() {
	hub := NewHub()
	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		HandleWebSocketConnection(hub, w, r)
	})

	port := "8080"
	fmt.Println("WebSocket server running on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
