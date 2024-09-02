-- MySQL ecommerce schema + seed data (~50MB total)
-- Run: mysql -u root ecommerce < mysql_seed.sql

-- Drop existing tables
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS users;

-- Users table (~10MB: 100K rows × ~100 bytes)
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Products table (~5MB: 50K rows × ~100 bytes)
CREATE TABLE products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    description TEXT,
    stock INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Orders table (~35MB: 500K rows × ~70 bytes)
CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_product (product_id)
) ENGINE=InnoDB;

-- Seed users (100K rows)
DELIMITER //
CREATE PROCEDURE seed_users()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 100000 DO
        INSERT INTO users (name, email) VALUES (
            CONCAT('User_', i),
            CONCAT('user', i, '@example.com')
        );
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;

-- Seed products (50K rows)
DELIMITER //
CREATE PROCEDURE seed_products()
BEGIN
    DECLARE i INT DEFAULT 1;
    WHILE i <= 50000 DO
        INSERT INTO products (name, price, description, stock) VALUES (
            CONCAT('Product_', i),
            ROUND(RAND() * 1000, 2),
            CONCAT('Description for product ', i, '. This is a sample product with various attributes.'),
            FLOOR(RAND() * 1000)
        );
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;

-- Seed orders (500K rows)
DELIMITER //
CREATE PROCEDURE seed_orders()
BEGIN
    DECLARE i INT DEFAULT 1;
    DECLARE qty INT;
    DECLARE price DECIMAL(10,2);
    WHILE i <= 500000 DO
        SET qty = FLOOR(1 + RAND() * 10);
        SET price = ROUND(RAND() * 500, 2);
        INSERT INTO orders (user_id, product_id, quantity, total) VALUES (
            FLOOR(1 + RAND() * 100000),
            FLOOR(1 + RAND() * 50000),
            qty,
            qty * price
        );
        SET i = i + 1;
    END WHILE;
END //
DELIMITER ;

-- Execute seeding
CALL seed_users();
CALL seed_products();
CALL seed_orders();

-- Cleanup procedures
DROP PROCEDURE seed_users;
DROP PROCEDURE seed_products;
DROP PROCEDURE seed_orders;

-- Show summary
SELECT 
    (SELECT COUNT(*) FROM users) as users_count,
    (SELECT COUNT(*) FROM products) as products_count,
    (SELECT COUNT(*) FROM orders) as orders_count;
