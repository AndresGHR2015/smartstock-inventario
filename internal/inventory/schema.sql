CREATE TABLE IF NOT EXISTS products (
  sku text PRIMARY KEY,
  name text NOT NULL,
  stock integer NOT NULL CHECK (stock >= 0),
  threshold integer NOT NULL CHECK (threshold >= 0)
);
CREATE TABLE IF NOT EXISTS sale_results (
  sale_id text PRIMARY KEY,
  sku text NOT NULL,
  quantity integer NOT NULL CHECK (quantity > 0),
  result integer NOT NULL,
  product jsonb
);
CREATE TABLE IF NOT EXISTS movements (
  sale_id text PRIMARY KEY REFERENCES sale_results(sale_id),
  sku text NOT NULL REFERENCES products(sku),
  quantity integer NOT NULL CHECK (quantity > 0),
  previous_stock integer NOT NULL,
  current_stock integer NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
  event_id text PRIMARY KEY,
  sku text NOT NULL,
  payload jsonb NOT NULL,
  published_at timestamptz
);
INSERT INTO products (sku, name, stock, threshold)
VALUES ('SKU-001', 'Producto de prueba', 10, 7)
ON CONFLICT (sku) DO NOTHING;
