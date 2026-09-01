# ************************************************************
# Antares - SQL Client
# Version 0.7.35
# 
# https://antares-sql.app/
# https://github.com/antares-sql/antares
# 
# Host: 127.0.0.1 (mariadb.org binary distribution 11.8.9)
# Database: Hooterdb
# Generation time: 2026-09-01T15:31:37+05:30
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
SET NAMES utf8mb4;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table brand
# ------------------------------------------------------------

DROP TABLE IF EXISTS `brand`;

CREATE TABLE `brand` (
  `brand_id` varchar(36) NOT NULL,
  `entity_name` varchar(255) NOT NULL,
  `brand_name` varchar(128) NOT NULL,
  `gstin` char(15) DEFAULT NULL,
  `poc` char(36) NOT NULL,
  `hooter_plan` enum('lite','pro','enterprise') DEFAULT NULL,
  `established_year` char(4) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `pincode` char(6) NOT NULL,
  `update_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `address` text NOT NULL,
  `city` varchar(128) NOT NULL,
  `state` varchar(128) NOT NULL,
  PRIMARY KEY (`brand_id`),
  UNIQUE KEY `brand_name` (`brand_name`),
  UNIQUE KEY `gstin` (`gstin`),
  KEY `poc` (`poc`),
  CONSTRAINT `brand_ibfk_1` FOREIGN KEY (`poc`) REFERENCES `user_creds` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

LOCK TABLES `brand` WRITE;
/*!40000 ALTER TABLE `brand` DISABLE KEYS */;

INSERT INTO `brand` (`brand_id`, `entity_name`, `brand_name`, `gstin`, `poc`, `hooter_plan`, `established_year`, `created_at`, `pincode`, `update_at`, `address`, `city`, `state`) VALUES
	("brand_dda509e9-8dc1-20260829", "ABC Pvt Ltd", "ABC Fashion", NULL, "user_08e23d19-f7ab-4b6620260827", "lite", "2023", "2026-08-29 06:55:19", "110025", "2026-08-29 06:55:19", "221B Baker Street", "New Delhi", "Delhi");

/*!40000 ALTER TABLE `brand` ENABLE KEYS */;
UNLOCK TABLES;



# Dump of table brand_access
# ------------------------------------------------------------

DROP TABLE IF EXISTS `brand_access`;

CREATE TABLE `brand_access` (
  `brand_id` varchar(36) NOT NULL,
  `user_id` char(36) NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_access` enum('brand_admin','brand_member','hooter_admin','hooter_member') NOT NULL DEFAULT 'brand_admin',
  PRIMARY KEY (`id`),
  UNIQUE KEY `user_id_2` (`user_id`,`brand_id`),
  KEY `brand_id` (`brand_id`),
  KEY `user_id` (`user_id`),
  CONSTRAINT `brand_access_ibfk_2` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`brand_id`) ON DELETE CASCADE,
  CONSTRAINT `brand_access_ibfk_3` FOREIGN KEY (`user_id`) REFERENCES `user_creds` (`user_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

LOCK TABLES `brand_access` WRITE;
/*!40000 ALTER TABLE `brand_access` DISABLE KEYS */;

INSERT INTO `brand_access` (`brand_id`, `user_id`, `id`, `user_access`) VALUES
	("brand_dda509e9-8dc1-20260829", "user_08e23d19-f7ab-4b6620260827", 1, "brand_admin"),
	("brand_dda509e9-8dc1-20260829", "user_8b72d4cd-d54b-41e120260829", 2, "brand_member");

/*!40000 ALTER TABLE `brand_access` ENABLE KEYS */;
UNLOCK TABLES;



# Dump of table catalog
# ------------------------------------------------------------

DROP TABLE IF EXISTS `catalog`;

CREATE TABLE `catalog` (
  `usku_id` varchar(64) NOT NULL,
  `product_title` varchar(200) NOT NULL,
  `price` decimal(10,2) NOT NULL,
  `compared_price` decimal(10,2) NOT NULL,
  `purchasing_cost` decimal(10,2) DEFAULT NULL,
  `vendor` varchar(128) DEFAULT NULL,
  `ean` varchar(13) DEFAULT NULL,
  `hsn` varchar(8) DEFAULT NULL,
  `gtin` varchar(14) DEFAULT NULL,
  `upc` varchar(12) DEFAULT NULL,
  `isbn` varchar(13) DEFAULT NULL,
  `net_weight_kg` decimal(6,2) DEFAULT NULL,
  `dead_weight_kg` decimal(6,2) DEFAULT NULL,
  `volumetric_weight_kg` decimal(6,2) DEFAULT NULL,
  `brand_name` varchar(255) NOT NULL,
  `tags` varchar(500) DEFAULT NULL,
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `product_desc` varchar(5000) NOT NULL,
  PRIMARY KEY (`usku_id`),
  CONSTRAINT `catalog_ibfk_1` FOREIGN KEY (`usku_id`) REFERENCES `usku_record` (`usku_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table catalogue_idempotency
# ------------------------------------------------------------

DROP TABLE IF EXISTS `catalogue_idempotency`;

CREATE TABLE `catalogue_idempotency` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `idempotency_key` varchar(100) NOT NULL,
  `user_id` varchar(36) NOT NULL,
  `brand_id` varchar(36) NOT NULL,
  `response_json` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`response_json`)),
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  KEY `brand_id` (`brand_id`),
  CONSTRAINT `catalogue_idempotency_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `user_creds` (`user_id`),
  CONSTRAINT `catalogue_idempotency_ibfk_2` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`brand_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table collection_records
# ------------------------------------------------------------

DROP TABLE IF EXISTS `collection_records`;

CREATE TABLE `collection_records` (
  `collection_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `brand_id` char(36) NOT NULL,
  PRIMARY KEY (`collection_id`),
  KEY `brand_id` (`brand_id`),
  CONSTRAINT `collection_records_ibfk_1` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`brand_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table grn
# ------------------------------------------------------------

DROP TABLE IF EXISTS `grn`;

CREATE TABLE `grn` (
  `grn_id` varchar(64) NOT NULL,
  `inward_id` int(11) NOT NULL,
  `created_at` timestamp NOT NULL,
  `inspection_status` enum('passed','failed','partial') NOT NULL,
  `damage_remarks` text DEFAULT NULL,
  `qc_notes` text DEFAULT NULL,
  PRIMARY KEY (`grn_id`),
  KEY `inward_id` (`inward_id`),
  CONSTRAINT `grn_ibfk_1` FOREIGN KEY (`inward_id`) REFERENCES `inward` (`inward_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table hsin_record
# ------------------------------------------------------------

DROP TABLE IF EXISTS `hsin_record`;

CREATE TABLE `hsin_record` (
  `usku_id` varchar(64) NOT NULL,
  `hsin` int(11) NOT NULL,
  `created_at` timestamp NOT NULL,
  `updated_at` timestamp NOT NULL,
  PRIMARY KEY (`usku_id`,`hsin`),
  CONSTRAINT `hsin_record_ibfk_1` FOREIGN KEY (`usku_id`) REFERENCES `usku_record` (`usku_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table inward
# ------------------------------------------------------------

DROP TABLE IF EXISTS `inward`;

CREATE TABLE `inward` (
  `inward_id` int(11) NOT NULL AUTO_INCREMENT,
  `supplier_id` int(11) NOT NULL,
  `inward_status` enum('pending','partial','completed','cancelled') NOT NULL DEFAULT 'pending',
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `brand_id` varchar(36) NOT NULL,
  `warehouse_id` int(11) NOT NULL,
  PRIMARY KEY (`inward_id`),
  KEY `supplier_id` (`supplier_id`),
  KEY `brand_id` (`brand_id`),
  KEY `warehouse_id` (`warehouse_id`),
  CONSTRAINT `inward_ibfk_1` FOREIGN KEY (`supplier_id`) REFERENCES `supplier` (`supplier_id`),
  CONSTRAINT `inward_ibfk_2` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`brand_id`) ON DELETE CASCADE,
  CONSTRAINT `inward_ibfk_4` FOREIGN KEY (`warehouse_id`) REFERENCES `warehouse` (`warehouse_id`)
) ENGINE=InnoDB AUTO_INCREMENT=11 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table inward_items
# ------------------------------------------------------------

DROP TABLE IF EXISTS `inward_items`;

CREATE TABLE `inward_items` (
  `index_id` int(11) NOT NULL AUTO_INCREMENT,
  `inward_id` int(11) NOT NULL,
  `usku_id` varchar(36) NOT NULL,
  `expected_qtt` int(11) NOT NULL DEFAULT 0,
  `received_qtt` int(11) NOT NULL DEFAULT 0,
  `rejected` int(11) NOT NULL DEFAULT 0,
  `po_num` varchar(64) DEFAULT NULL,
  `uom` enum('EA','PCS','PAC','BOX','CTN','CS','DZ','PAL','KG','G','LBS','OZ','TN','L','ML','GAL','FLOZ','M','CM','FT','M2','SQM') NOT NULL DEFAULT 'EA',
  `batch_num` varchar(64) DEFAULT NULL,
  `expiry_date` date DEFAULT NULL,
  PRIMARY KEY (`index_id`),
  KEY `inward_id` (`inward_id`),
  KEY `usku_id` (`usku_id`),
  CONSTRAINT `inward_items_ibfk_1` FOREIGN KEY (`inward_id`) REFERENCES `inward` (`inward_id`) ON DELETE CASCADE,
  CONSTRAINT `inward_items_ibfk_2` FOREIGN KEY (`usku_id`) REFERENCES `usku_record` (`usku_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table product_collections
# ------------------------------------------------------------

DROP TABLE IF EXISTS `product_collections`;

CREATE TABLE `product_collections` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `collection_id` int(11) NOT NULL,
  `usku_id` varchar(64) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `collection_id` (`collection_id`,`usku_id`),
  KEY `usku_id` (`usku_id`),
  CONSTRAINT `product_collections_ibfk_1` FOREIGN KEY (`collection_id`) REFERENCES `collection_records` (`collection_id`),
  CONSTRAINT `product_collections_ibfk_2` FOREIGN KEY (`usku_id`) REFERENCES `usku_record` (`usku_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table product_images
# ------------------------------------------------------------

DROP TABLE IF EXISTS `product_images`;

CREATE TABLE `product_images` (
  `usku_id` varchar(64) NOT NULL,
  `image_url` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`image_url`)),
  `image_type` varchar(32) NOT NULL,
  `image_order` char(1) NOT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`id`),
  UNIQUE KEY `usku_id` (`usku_id`,`image_type`),
  CONSTRAINT `product_images_ibfk_1` FOREIGN KEY (`usku_id`) REFERENCES `usku_record` (`usku_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=185 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table product_info_change_stack
# ------------------------------------------------------------

DROP TABLE IF EXISTS `product_info_change_stack`;

CREATE TABLE `product_info_change_stack` (
  `change_id` int(11) NOT NULL AUTO_INCREMENT,
  `usku_id` varchar(64) NOT NULL,
  `user_id` varchar(36) NOT NULL,
  `updated_at` timestamp NOT NULL,
  `changed_attribute` text NOT NULL,
  PRIMARY KEY (`change_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table shipment
# ------------------------------------------------------------

DROP TABLE IF EXISTS `shipment`;

CREATE TABLE `shipment` (
  `shipment_id` int(11) NOT NULL AUTO_INCREMENT,
  `shipment_ref_no` varchar(64) DEFAULT NULL,
  `vehicle_no` varchar(36) DEFAULT NULL,
  `transporter` varchar(128) NOT NULL,
  `delivery_challan` varchar(64) DEFAULT NULL,
  `arrival_date` date NOT NULL,
  `inward_id` int(11) NOT NULL,
  PRIMARY KEY (`shipment_id`),
  UNIQUE KEY `shipment_ref_no` (`shipment_ref_no`),
  KEY `inward_id` (`inward_id`),
  CONSTRAINT `shipment_ibfk_1` FOREIGN KEY (`inward_id`) REFERENCES `inward` (`inward_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table shopify_product_mapping
# ------------------------------------------------------------

DROP TABLE IF EXISTS `shopify_product_mapping`;

CREATE TABLE `shopify_product_mapping` (
  `mapping_id` int(11) NOT NULL AUTO_INCREMENT,
  `usku_id` varchar(64) NOT NULL,
  `store_id` int(11) NOT NULL,
  `shopify_product_id` varchar(100) NOT NULL,
  `last_sync_status` enum('SUCCESS','FAILED') DEFAULT NULL,
  `sync_error_message` text DEFAULT NULL,
  `synced_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`mapping_id`),
  KEY `usku_id` (`usku_id`),
  KEY `store_id` (`store_id`),
  CONSTRAINT `shopify_product_mapping_ibfk_1` FOREIGN KEY (`usku_id`) REFERENCES `usku_record` (`usku_id`) ON DELETE CASCADE,
  CONSTRAINT `shopify_product_mapping_ibfk_2` FOREIGN KEY (`store_id`) REFERENCES `shopify_stores` (`store_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table shopify_stores
# ------------------------------------------------------------

DROP TABLE IF EXISTS `shopify_stores`;

CREATE TABLE `shopify_stores` (
  `store_id` int(11) NOT NULL AUTO_INCREMENT,
  `brand_id` varchar(36) NOT NULL,
  `shopify_shop_name` varchar(255) NOT NULL,
  `shopify_access_token_encrypted` varchar(500) NOT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT NULL ON UPDATE current_timestamp(),
  PRIMARY KEY (`store_id`),
  UNIQUE KEY `shopify_shop_name` (`shopify_shop_name`),
  KEY `brand_id` (`brand_id`),
  CONSTRAINT `shopify_stores_ibfk_1` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`brand_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table supplier
# ------------------------------------------------------------

DROP TABLE IF EXISTS `supplier`;

CREATE TABLE `supplier` (
  `supplier_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(36) NOT NULL,
  `contact_number` varchar(12) NOT NULL,
  `address` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`address`)),
  `email` varchar(255) DEFAULT NULL,
  `brand_id` varchar(36) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`supplier_id`),
  UNIQUE KEY `contact_number` (`contact_number`),
  UNIQUE KEY `email` (`email`),
  KEY `brand_id` (`brand_id`),
  CONSTRAINT `supplier_ibfk_1` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`brand_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table user_creds
# ------------------------------------------------------------

DROP TABLE IF EXISTS `user_creds`;

CREATE TABLE `user_creds` (
  `user_id` char(36) NOT NULL,
  `user_name` varchar(36) NOT NULL,
  `phone_number` varchar(11) NOT NULL,
  `user_email` varchar(256) NOT NULL,
  `user_designation` varchar(64) NOT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `phone_number` (`phone_number`),
  UNIQUE KEY `user_email` (`user_email`),
  CONSTRAINT `user_creds_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

LOCK TABLES `user_creds` WRITE;
/*!40000 ALTER TABLE `user_creds` DISABLE KEYS */;

INSERT INTO `user_creds` (`user_id`, `user_name`, `phone_number`, `user_email`, `user_designation`, `created_at`) VALUES
	("user_08e23d19-f7ab-4b6620260827", "Farhan Ahmad", "7836815466", "farhanahmadpy@gmail.com", "Owner", "2026-08-27 00:00:00"),
	("user_8b72d4cd-d54b-41e120260829", "John Doe", "9876543210", "john@example.com", "Manager", "2026-08-29 00:00:00");

/*!40000 ALTER TABLE `user_creds` ENABLE KEYS */;
UNLOCK TABLES;



# Dump of table users
# ------------------------------------------------------------

DROP TABLE IF EXISTS `users`;

CREATE TABLE `users` (
  `user_id` char(36) NOT NULL,
  `user_password` varchar(255) NOT NULL,
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;

INSERT INTO `users` (`user_id`, `user_password`) VALUES
	("user_08e23d19-f7ab-4b6620260827", "$argon2id$v=19$m=65536,t=3,p=4$QOeGTGrPvd6DXH7PShw6aA$RHJyScGiCePdYTrsH2stfLsajynNz5UFVkpslhjue0k"),
	("user_8b72d4cd-d54b-41e120260829", "$argon2id$v=19$m=65536,t=3,p=4$ePnc0pugH5IXORpDqgdJ7Q$meevabIzJTEmoBHfoHqHiMYeaQY7IBq1AZuehWWJMyc");

/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;



# Dump of table usku_record
# ------------------------------------------------------------

DROP TABLE IF EXISTS `usku_record`;

CREATE TABLE `usku_record` (
  `usku_id` varchar(64) NOT NULL,
  `brand_id` varchar(36) NOT NULL,
  `sku_id` varchar(100) NOT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `type_id` varchar(64) NOT NULL,
  `status` enum('pending','completed') NOT NULL DEFAULT 'pending',
  `indx` int(11) NOT NULL AUTO_INCREMENT,
  `type_name` varchar(100) NOT NULL,
  `stock` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`indx`),
  UNIQUE KEY `brand_id` (`brand_id`,`sku_id`),
  UNIQUE KEY `usku_id` (`usku_id`),
  CONSTRAINT `usku_record_ibfk_2` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`brand_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=594 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table variants
# ------------------------------------------------------------

DROP TABLE IF EXISTS `variants`;

CREATE TABLE `variants` (
  `indx` int(11) NOT NULL AUTO_INCREMENT,
  `usku_id` varchar(64) NOT NULL,
  `variant_id` varchar(72) NOT NULL,
  PRIMARY KEY (`indx`),
  UNIQUE KEY `usku_id` (`usku_id`,`variant_id`),
  CONSTRAINT `variants_ibfk_1` FOREIGN KEY (`usku_id`) REFERENCES `usku_record` (`usku_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=27 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of table warehouse
# ------------------------------------------------------------

DROP TABLE IF EXISTS `warehouse`;

CREATE TABLE `warehouse` (
  `warehouse_id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `address` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(`address`)),
  `phone` varchar(20) NOT NULL,
  `email` varchar(255) NOT NULL,
  `status` enum('active','inactive') NOT NULL DEFAULT 'active',
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  `brand_id` varchar(36) NOT NULL,
  PRIMARY KEY (`warehouse_id`),
  UNIQUE KEY `phone` (`phone`),
  UNIQUE KEY `email` (`email`),
  KEY `brand_id` (`brand_id`),
  CONSTRAINT `warehouse_ibfk_1` FOREIGN KEY (`brand_id`) REFERENCES `brand` (`brand_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;





# Dump of views
# ------------------------------------------------------------

# Creating temporary tables to overcome VIEW dependency errors


/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;

# Dump completed on 2026-09-01T15:31:37+05:30
