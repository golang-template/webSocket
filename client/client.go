package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/gorilla/websocket"
)

type WebSocketClient struct {
	Conn *websocket.Conn
}

func NewWebSocketClient(serverURL string) (*WebSocketClient, error) {
	conn, _, err := websocket.DefaultDialer.Dial(serverURL, nil)
	if err != nil {
		return nil, err
	}
	fmt.Println(" Connected to WebSocket server")
	return &WebSocketClient{Conn: conn}, nil
}

// listens for messages from the WebSocket server
func (c *WebSocketClient) ReadMessages() {
	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			log.Println(" Read error:", err)
			return
		}
		fmt.Println(" Received:", string(message))
	}
}

// sends a message to the WebSocket server
func (c *WebSocketClient) SendMessage(message string) error {
	return c.Conn.WriteMessage(websocket.TextMessage, []byte(message))
}

func main() {
	serverURL := "ws://localhost:8080/ws"

	client, err := NewWebSocketClient(serverURL)
	if err != nil {
		log.Fatal(" Connection error:", err)
	}
	defer client.Conn.Close()

	// Handle incoming messages
	go client.ReadMessages()

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println(" Type a message and press ENTER to send (type 'exit' to quit):")

	go func() {
		for scanner.Scan() {
			message := scanner.Text()
			if message == "exit" {
				fmt.Println(" Exiting WebSocket client...")
				client.Conn.Close()
				os.Exit(0)
			}
			err := client.SendMessage(message)
			if err != nil {
				log.Println(" Send error:", err)
				return
			}
			fmt.Println(" Message sent!")
		}
	}()

	// Handle exit signals (CTRL+C)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	fmt.Println("\n Exiting WebSocket client...")
}
