package inventory

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "smartstock/inventory/gen/inventory/v1"
)

//go:embed schema.sql
var schema string

type Service struct {
	pb.UnimplementedInventoryServiceServer
	DB *pgxpool.Pool
}

func Migrate(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, schema)
	return err
}

func valid(value string) bool { return strings.TrimSpace(value) != "" }

func (s *Service) GetProduct(ctx context.Context, request *pb.GetProductRequest) (*pb.GetProductResponse, error) {
	if !valid(request.GetSku()) {
		return &pb.GetProductResponse{Result: pb.GetProductResponse_INVALID_SKU}, nil
	}
	product := &pb.Product{}
	err := s.DB.QueryRow(ctx, "SELECT sku, name, stock, threshold FROM products WHERE sku=$1", request.Sku).
		Scan(&product.Sku, &product.Name, &product.Stock, &product.Threshold)
	if err == pgx.ErrNoRows {
		return &pb.GetProductResponse{Result: pb.GetProductResponse_NOT_FOUND}, nil
	}
	if err != nil {
		return nil, status.Error(codes.Internal, "error consultando producto")
	}
	return &pb.GetProductResponse{Result: pb.GetProductResponse_FOUND, Product: product}, nil
}

func (s *Service) GetInventoryOverview(ctx context.Context, _ *pb.GetInventoryOverviewRequest) (*pb.GetInventoryOverviewResponse, error) {
	const query = `
		SELECT totals.critical, attention.sku, attention.name, attention.stock, attention.threshold
		FROM (SELECT count(*)::integer AS critical FROM products WHERE stock <= threshold) totals
		LEFT JOIN LATERAL (
			SELECT sku, name, stock, threshold FROM products
			WHERE stock::bigint * 2 <= threshold::bigint * 3
			ORDER BY (stock <= threshold) DESC, stock - threshold, sku
			LIMIT 4
		) attention ON true
		ORDER BY (attention.stock <= attention.threshold) DESC, attention.stock - attention.threshold, attention.sku
	`
	rows, err := s.DB.Query(ctx, query)
	if err != nil {
		return nil, status.Error(codes.Internal, "error consultando resumen de inventario")
	}
	defer rows.Close()

	response := &pb.GetInventoryOverviewResponse{}
	for rows.Next() {
		product := &pb.Product{}
		var sku *string
		var name *string
		var stock, threshold *int32
		if err := rows.Scan(&response.CriticalCount, &sku, &name, &stock, &threshold); err != nil {
			return nil, status.Error(codes.Internal, "error leyendo resumen de inventario")
		}
		if sku != nil {
			product.Sku, product.Name = *sku, *name
			product.Stock, product.Threshold = *stock, *threshold
			response.Attention = append(response.Attention, product)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, status.Error(codes.Internal, "error leyendo resumen de inventario")
	}
	return response, nil
}

func (s *Service) DeductStock(ctx context.Context, request *pb.DeductStockRequest) (*pb.DeductStockResponse, error) {
	if !valid(request.GetSaleId()) || !valid(request.GetSku()) || request.GetQuantity() <= 0 {
		return &pb.DeductStockResponse{Result: pb.DeductStockResponse_INVALID_ARGUMENT}, nil
	}
	response, err := s.deduct(ctx, request)
	if err != nil {
		return nil, status.Error(codes.Internal, "error descontando inventario")
	}
	return response, nil
}

func (s *Service) deduct(ctx context.Context, request *pb.DeductStockRequest) (*pb.DeductStockResponse, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Serializes even requests for a sale ID that does not yet exist.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", request.SaleId); err != nil {
		return nil, err
	}
	var sku string
	var quantity, result int32
	var snapshot []byte
	err = tx.QueryRow(ctx, "SELECT sku, quantity, result, product FROM sale_results WHERE sale_id=$1", request.SaleId).
		Scan(&sku, &quantity, &result, &snapshot)
	if err == nil {
		if sku != request.Sku || quantity != request.Quantity {
			return &pb.DeductStockResponse{Result: pb.DeductStockResponse_SALE_ID_CONFLICT}, nil
		}
		response := &pb.DeductStockResponse{Result: pb.DeductStockResponse_Result(result)}
		if len(snapshot) != 0 {
			response.Product = &pb.Product{}
			if err := json.Unmarshal(snapshot, response.Product); err != nil {
				return nil, err
			}
		}
		return response, nil
	}
	if err != pgx.ErrNoRows {
		return nil, err
	}

	product := &pb.Product{}
	err = tx.QueryRow(ctx, "SELECT sku, name, stock, threshold FROM products WHERE sku=$1 FOR UPDATE", request.Sku).
		Scan(&product.Sku, &product.Name, &product.Stock, &product.Threshold)
	response := &pb.DeductStockResponse{}
	switch {
	case err == pgx.ErrNoRows:
		response.Result = pb.DeductStockResponse_PRODUCT_NOT_FOUND
	case err != nil:
		return nil, err
	case product.Stock < request.Quantity:
		response.Result = pb.DeductStockResponse_INSUFFICIENT_STOCK
	default:
		previous := product.Stock
		product.Stock -= request.Quantity
		if _, err = tx.Exec(ctx, "UPDATE products SET stock=$1 WHERE sku=$2", product.Stock, product.Sku); err != nil {
			return nil, err
		}
		response.Result = pb.DeductStockResponse_DEDUCTED
		response.Product = product
		snapshot, err = json.Marshal(product)
		if err != nil {
			return nil, err
		}
		if previous > product.Threshold && product.Stock <= product.Threshold {
			if err = insertEvent(ctx, tx, request.SaleId, product, previous); err != nil {
				return nil, err
			}
		}
	}
	if _, err = tx.Exec(ctx, "INSERT INTO sale_results (sale_id,sku,quantity,result,product) VALUES ($1,$2,$3,$4,$5)",
		request.SaleId, request.Sku, request.Quantity, int32(response.Result), snapshot); err != nil {
		return nil, err
	}
	if response.Product != nil {
		_, err = tx.Exec(ctx, "INSERT INTO movements (sale_id,sku,quantity,previous_stock,current_stock) VALUES ($1,$2,$3,$4,$5)",
			request.SaleId, request.Sku, request.Quantity, product.Stock+request.Quantity, product.Stock)
		if err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return response, nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, saleID string, product *pb.Product, previous int32) error {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	eventID := hex.EncodeToString(id[:])
	event := struct {
		EventID       string `json:"event_id"`
		EventType     string `json:"event_type"`
		EventVersion  int    `json:"event_version"`
		OccurredAt    string `json:"occurred_at"`
		SKU           string `json:"sku"`
		PreviousStock int32  `json:"previous_stock"`
		CurrentStock  int32  `json:"current_stock"`
		Threshold     int32  `json:"threshold"`
		SaleID        string `json:"sale_id"`
	}{eventID, "inventory.stock_low", 1, time.Now().UTC().Format(time.RFC3339Nano), product.Sku, previous, product.Stock, product.Threshold, saleID}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO outbox (event_id,sku,payload) VALUES ($1,$2,$3)", eventID, product.Sku, payload)
	if err != nil {
		return fmt.Errorf("insertar evento: %w", err)
	}
	return nil
}
