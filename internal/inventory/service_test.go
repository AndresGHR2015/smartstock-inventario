package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	pb "smartstock/inventory/gen/inventory/v1"
)

type fakePublisher struct {
	messages []kafka.Message
	fail     bool
}

func TestLiveServer(t *testing.T) {
	address := os.Getenv("TEST_GRPC_ADDR")
	if address == "" {
		t.Skip("TEST_GRPC_ADDR no configurada")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewInventoryServiceClient(conn)
	product, err := client.GetProduct(ctx, &pb.GetProductRequest{Sku: "SKU-001"})
	if err != nil || product.GetProduct().GetStock() != 10 {
		t.Fatalf("stock inicial: %v, %v", product, err)
	}
	request := &pb.DeductStockRequest{SaleId: "live-sale-001", Sku: "SKU-001", Quantity: 3}
	first, err := client.DeductStock(ctx, request)
	if err != nil || first.GetResult() != pb.DeductStockResponse_DEDUCTED || first.GetProduct().GetStock() != 7 {
		t.Fatalf("descuento: %v, %v", first, err)
	}
	repeated, err := client.DeductStock(ctx, request)
	if err != nil || repeated.GetResult() != pb.DeductStockResponse_DEDUCTED || repeated.GetProduct().GetStock() != 7 {
		t.Fatalf("reintento: %v, %v", repeated, err)
	}
	t.Log("stock inicial=10, descuento=7, repetición=7")
}

type lostAckPublisher struct {
	writer *kafka.Writer
	fail   bool
}

func (p *lostAckPublisher) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	if err := p.writer.WriteMessages(ctx, messages...); err != nil {
		return err
	}
	if p.fail {
		p.fail = false
		return errors.New("ack perdido tras publicación")
	}
	return nil
}

func TestKafkaRedelivery(t *testing.T) {
	url, broker := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_KAFKA_BROKER")
	if url == "" || broker == "" {
		t.Skip("TEST_DATABASE_URL y TEST_KAFKA_BROKER son necesarios")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprint(time.Now().UnixNano())
	sku, saleID := "kafka-"+suffix, "sale-"+suffix
	if _, err := db.Exec(ctx, "INSERT INTO products (sku,name,stock,threshold) VALUES ($1,'test',10,7)", sku); err != nil {
		t.Fatal(err)
	}
	service := &Service{DB: db}
	response, err := service.DeductStock(ctx, &pb.DeductStockRequest{SaleId: saleID, Sku: sku, Quantity: 3})
	if err != nil || response.GetResult() != pb.DeductStockResponse_DEDUCTED {
		t.Fatalf("descuento: %v %v", response, err)
	}
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: "inventory.stock_low.v1", RequiredAcks: kafka.RequireAll}
	defer writer.Close()
	publisher := &lostAckPublisher{writer: writer, fail: true}
	if _, err := PublishOne(ctx, db, publisher); err == nil {
		t.Fatal("se esperaba fallo tras publicación")
	}
	if published, err := PublishOne(ctx, db, publisher); err != nil || !published {
		t.Fatalf("reintento: %v %v", published, err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker}, Topic: "inventory.stock_low.v1", Partition: 0, MinBytes: 1, MaxBytes: 10e6,
	})
	defer reader.Close()
	var eventID string
	found := 0
	for found < 2 {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if string(message.Key) != sku {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal(message.Value, &event); err != nil {
			t.Fatal(err)
		}
		id, _ := event["event_id"].(string)
		if id == "" || (eventID != "" && eventID != id) || event["sale_id"] != saleID {
			t.Fatalf("evento inesperado: %v", event)
		}
		eventID = id
		found++
	}
}

func (p *fakePublisher) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	p.messages = append(p.messages, messages...)
	if p.fail {
		return errors.New("ack perdido")
	}
	return nil
}

func TestIntegration(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL no configurada")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(ctx, "DROP TABLE IF EXISTS outbox, movements, sale_results, products CASCADE"); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterInventoryServiceServer(server, &Service{DB: db})
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///inventory", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewInventoryServiceClient(conn)
	get := func(sku string) *pb.GetProductResponse {
		t.Helper()
		response, err := client.GetProduct(ctx, &pb.GetProductRequest{Sku: sku})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	deduct := func(id, sku string, quantity int32) *pb.DeductStockResponse {
		t.Helper()
		response, err := client.DeductStock(ctx, &pb.DeductStockRequest{SaleId: id, Sku: sku, Quantity: quantity})
		if err != nil {
			t.Fatal(err)
		}
		return response
	}
	if response := get("SKU-001"); response.GetProduct().GetStock() != 10 {
		t.Fatalf("stock inicial: %v", response)
	}
	if get(" ").Result != pb.GetProductResponse_INVALID_SKU || get("missing").Result != pb.GetProductResponse_NOT_FOUND {
		t.Fatal("resultados de consulta inválidos")
	}
	for _, quantity := range []int32{0, -1} {
		if deduct("invalid", "SKU-001", quantity).Result != pb.DeductStockResponse_INVALID_ARGUMENT {
			t.Fatal("cantidad inválida aceptada")
		}
	}
	if deduct(" ", "SKU-001", 1).Result != pb.DeductStockResponse_INVALID_ARGUMENT ||
		deduct("invalid", " ", 1).Result != pb.DeductStockResponse_INVALID_ARGUMENT {
		t.Fatal("identificador inválido aceptado")
	}
	if deduct("missing", "missing", 1).Result != pb.DeductStockResponse_PRODUCT_NOT_FOUND ||
		deduct("insufficient", "SKU-001", 11).Result != pb.DeductStockResponse_INSUFFICIENT_STOCK {
		t.Fatal("rechazo esperado")
	}
	first := deduct("sale-1", "SKU-001", 3)
	if first.Result != pb.DeductStockResponse_DEDUCTED || first.GetProduct().GetStock() != 7 {
		t.Fatalf("descuento: %v", first)
	}
	if deduct("sale-1", "SKU-001", 3).GetProduct().GetStock() != 7 ||
		deduct("sale-1", "SKU-001", 2).Result != pb.DeductStockResponse_SALE_ID_CONFLICT ||
		deduct("sale-1", "other", 3).Result != pb.DeductStockResponse_SALE_ID_CONFLICT {
		t.Fatal("idempotencia o conflicto incorrecto")
	}
	if err := Migrate(ctx, db); err != nil || get("SKU-001").GetProduct().GetStock() != 7 {
		t.Fatal("la migración restauró el stock", err)
	}
	if deduct("insufficient", "SKU-001", 11).Result != pb.DeductStockResponse_INSUFFICIENT_STOCK {
		t.Fatal("el rechazo no se conservó")
	}
	if deduct("insufficient", "SKU-001", 1).Result != pb.DeductStockResponse_SALE_ID_CONFLICT {
		t.Fatal("un rechazo debe reservar el identificador")
	}
	if _, err := db.Exec(ctx, "INSERT INTO products (sku,name,stock,threshold) VALUES ('missing','added',10,0)"); err != nil {
		t.Fatal(err)
	}
	if deduct("missing", "missing", 1).Result != pb.DeductStockResponse_PRODUCT_NOT_FOUND {
		t.Fatal("el rechazo por producto inexistente no se conservó")
	}
	publisher := &fakePublisher{fail: true}
	if _, err := PublishOne(ctx, db, publisher); err == nil {
		t.Fatal("se esperaba fallo del publicador")
	}
	publisher.fail = false
	if published, err := PublishOne(ctx, db, publisher); err != nil || !published {
		t.Fatalf("reintento: %v %v", published, err)
	}
	if len(publisher.messages) != 2 || string(publisher.messages[0].Value) != string(publisher.messages[1].Value) ||
		string(publisher.messages[1].Key) != "SKU-001" {
		t.Fatal("duplicado no estable")
	}
	var event map[string]any
	if err := json.Unmarshal(publisher.messages[1].Value, &event); err != nil {
		t.Fatal(err)
	}
	if event["sale_id"] != "sale-1" || event["previous_stock"] != float64(10) || event["current_stock"] != float64(7) || event["threshold"] != float64(7) {
		t.Fatalf("evento incorrecto: %v", event)
	}
	if published, err := PublishOne(ctx, db, publisher); err != nil || published {
		t.Fatalf("outbox no vacío: %v %v", published, err)
	}
	var wg sync.WaitGroup
	results := make(chan pb.DeductStockResponse_Result, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			response, err := client.DeductStock(ctx, &pb.DeductStockRequest{SaleId: fmt.Sprintf("concurrent-%d", i), Sku: "SKU-001", Quantity: 1})
			if err != nil {
				t.Errorf("descuento concurrente: %v", err)
				return
			}
			results <- response.Result
		}(i)
	}
	wg.Wait()
	close(results)
	successes := 0
	for result := range results {
		if result == pb.DeductStockResponse_DEDUCTED {
			successes++
		} else if result != pb.DeductStockResponse_INSUFFICIENT_STOCK {
			t.Fatalf("resultado inesperado: %v", result)
		}
	}
	if successes != 7 || get("SKU-001").GetProduct().GetStock() != 0 {
		t.Fatalf("concurrencia: %d descuentos", successes)
	}
	if deduct("sale-1", "SKU-001", 3).GetProduct().GetStock() != 7 {
		t.Fatal("el reintento no devolvió la instantánea original")
	}
	var movements, events int
	if err := db.QueryRow(ctx, "SELECT (SELECT count(*) FROM movements), (SELECT count(*) FROM outbox)").Scan(&movements, &events); err != nil {
		t.Fatal(err)
	}
	if movements != 8 || events != 1 {
		t.Fatalf("movimientos=%d eventos=%d", movements, events)
	}
	results = make(chan pb.DeductStockResponse_Result, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			response, err := client.DeductStock(ctx, &pb.DeductStockRequest{SaleId: "same-sale", Sku: "missing", Quantity: 3})
			if err != nil {
				t.Errorf("misma venta concurrente: %v", err)
				return
			}
			results <- response.Result
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result != pb.DeductStockResponse_DEDUCTED {
			t.Fatalf("resultado de venta concurrente: %v", result)
		}
	}
	if get("missing").GetProduct().GetStock() != 7 {
		t.Fatal("la misma venta se descontó más de una vez")
	}
	db.Close()
	_, err = client.GetProduct(ctx, &pb.GetProductRequest{Sku: "SKU-001"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("el fallo técnico de consulta no es gRPC Internal: %v", err)
	}
	_, err = client.DeductStock(ctx, &pb.DeductStockRequest{SaleId: "technical", Sku: "SKU-001", Quantity: 1})
	if status.Code(err) != codes.Internal {
		t.Fatalf("el fallo técnico de descuento no es gRPC Internal: %v", err)
	}
}
