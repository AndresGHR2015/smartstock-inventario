package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	pb "smartstock/inventory/gen/inventory/v1"
	"smartstock/inventory/internal/inventory"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	databaseURL := os.Getenv("DATABASE_URL")
	brokers := os.Getenv("KAFKA_BROKERS")
	if databaseURL == "" || brokers == "" {
		log.Fatal("DATABASE_URL y KAFKA_BROKERS son obligatorios")
	}
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := inventory.Migrate(ctx, db); err != nil {
		log.Fatal(err)
	}
	address := os.Getenv("GRPC_ADDR")
	if address == "" {
		address = ":50051"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	writer := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(brokers, ",")...),
		Topic:        "inventory.stock_low.v1",
		RequiredAcks: kafka.RequireAll,
	}
	defer writer.Close()
	go func() {
		for ctx.Err() == nil {
			published, err := inventory.PublishOne(ctx, db, writer)
			if err != nil && ctx.Err() == nil {
				log.Printf("error publicando outbox: %v", err)
			}
			if err != nil || !published {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		}
	}()
	server := grpc.NewServer()
	pb.RegisterInventoryServiceServer(server, &inventory.Service{DB: db})
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()
	log.Printf("gRPC escuchando en %s", address)
	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
