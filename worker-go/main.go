package main

import (
    "encoding/json"
    "log"

    "github.com/streadway/amqp"
)

type WeatherData struct {
    Temperature float64 `json:"temperature"`
    Humidity    float64 `json:"humidity"`
    WindSpeed   float64 `json:"wind_speed"`
    Condition   string  `json:"condition"`
}

func main() {
    conn, err := amqp.Dial("amqp://admin:admin@rabbitmq:5672/")
    if err != nil {
        log.Fatalf("Failed to connect to RabbitMQ: %v", err)
    }
    defer conn.Close()

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Failed to open channel: %v", err)
    }
    defer ch.Close()

    _, err = ch.QueueDeclare(
        "weather_queue",
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatalf("Failed to declare queue: %v", err)
    }

    msgs, err := ch.Consume(
        "weather_queue",
        "",
        true,
        false,
        false,
        false,
        nil,
    )
    if err != nil {
        log.Fatalf("Failed to register consumer: %v", err)
    }

    log.Println("Worker Go iniciado... aguardando mensagens.")

    for msg := range msgs {
        var data WeatherData
        json.Unmarshal(msg.Body, &data)

        log.Printf("[WORKER GO] Mensagem recebida: %+v\n", data)
    }
}
