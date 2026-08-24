-- 002_seed.sql — sample data for the shop schema.
-- Assumes 001_shop.sql ran first. Rerunnable: truncates before inserting.

SET search_path TO shop;

TRUNCATE order_items, orders, products, customers RESTART IDENTITY CASCADE;

INSERT INTO customers (name, email, country, signup_date) VALUES
  ('Aarti Sharma',   'aarti.sharma@example.com',   'IN', '2025-01-14'),
  ('Ben Okafor',     'ben.okafor@example.com',     'NG', '2025-02-02'),
  ('Clara Nilsson',  'clara.nilsson@example.com',  'SE', '2025-02-19'),
  ('Diego Ramirez',  'diego.ramirez@example.com',  'MX', '2025-03-08'),
  ('Emma Whitfield', 'emma.whitfield@example.com', 'GB', '2025-03-30'),
  ('Farhan Iqbal',   'farhan.iqbal@example.com',   'PK', '2025-04-11'),
  ('Grace Lim',      'grace.lim@example.com',      'SG', '2025-05-05'),
  ('Hiroshi Tanaka', 'hiroshi.tanaka@example.com', 'JP', '2025-05-27'),
  ('Ines Duarte',    'ines.duarte@example.com',    'PT', '2025-06-15'),
  ('Jonas Meyer',    'jonas.meyer@example.com',    'DE', '2025-07-01');

INSERT INTO products (sku, name, category, price) VALUES
  ('NOVA-KB-001', 'Mechanical Keyboard 75%', 'peripherals', 129.00),
  ('NOVA-MS-002', 'Wireless Mouse',          'peripherals',  49.50),
  ('NOVA-MN-003', '27in 1440p Monitor',      'displays',    319.99),
  ('NOVA-MN-004', '34in Ultrawide Monitor',  'displays',    579.00),
  ('NOVA-HP-005', 'Over-ear Headphones',     'audio',       199.95),
  ('NOVA-SP-006', 'Desk Speakers Pair',      'audio',        89.00),
  ('NOVA-DK-007', 'Standing Desk 120cm',     'furniture',   449.00),
  ('NOVA-CH-008', 'Ergonomic Chair',         'furniture',   612.75),
  ('NOVA-CB-009', 'USB-C Hub 8-in-1',        'accessories',  39.99),
  ('NOVA-LP-010', 'Laptop Stand Aluminium',  'accessories',  27.50);

INSERT INTO orders (customer_id, ordered_at, status) VALUES
  ( 1, '2025-07-02 09:14:00+00', 'completed'),
  ( 2, '2025-07-03 16:40:00+00', 'completed'),
  ( 3, '2025-07-05 11:02:00+00', 'pending'),
  ( 4, '2025-07-08 20:31:00+00', 'cancelled'),
  ( 5, '2025-07-11 07:55:00+00', 'completed'),
  ( 6, '2025-07-15 13:20:00+00', 'refunded'),
  ( 7, '2025-07-19 18:05:00+00', 'completed'),
  ( 8, '2025-07-23 10:47:00+00', 'pending'),
  ( 9, '2025-07-28 15:12:00+00', 'completed'),
  (10, '2025-08-01 08:00:00+00', 'completed');

INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES
  ( 1,  1, 1, 129.00),
  ( 1,  2, 2,  49.50),
  ( 2,  3, 1, 319.99),
  ( 3,  5, 1, 199.95),
  ( 3,  9, 3,  39.99),
  ( 4,  8, 1, 612.75),
  ( 5,  4, 1, 559.00),  -- sold under list price: promo at order time
  ( 5, 10, 2,  27.50),
  ( 6,  6, 1,  89.00),
  ( 7,  7, 1, 449.00),
  ( 7,  9, 1,  39.99),
  ( 8,  2, 1,  49.50),
  ( 9,  1, 2, 129.00),
  ( 9,  5, 1, 199.95),
  (10,  3, 2, 319.99);
