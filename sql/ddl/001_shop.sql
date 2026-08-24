DROP SCHEMA IF EXISTS shop CASCADE;
CREATE SCHEMA shop;
SET search_path TO shop;

CREATE TABLE customers (
	id 		SERIAL PRIMARY KEY,
	name 		TEXT NOT NULL,
	email		TEXT UNIQUE NOT NULL,
	country		TEXT NOT NULL,
	signup_date	DATE NOT NULL
);

CREATE TABLE products (
	id 		SERIAL PRIMARY KEY,
	sku		TEXT UNIQUE NOT NULL,
	name		TEXT NOT NULL,
	category	TEXT NOT NULL,
	price		NUMERIC(10,2) NOT NULL CHECK(price >= 0) 

);

CREATE TABLE orders (
	id		 SERIAL PRIMARY KEY,
	customer_id	 INT NOT NULL REFERENCES customers(id),
	ordered_at	 TIMESTAMPTZ NOT NULL,
	status		 TEXT NOT NULL
	CHECK		 (status IN ('pending','completed','cancelled','refunded'))
);

CREATE TABLE order_items (
	id		 SERIAL PRIMARY KEY,
	order_id	 INT NOT NULL REFERENCES orders(id),
	product_id	 INT NOT NULL REFERENCES products(id),
	quantity	 INT NOT NULL CHECK (quantity > 0),
	unit_price	 NUMERIC(10,2) NOT NULL
);

