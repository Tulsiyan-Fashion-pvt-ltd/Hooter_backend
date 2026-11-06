drop table variants;

CREATE TABLE `product_variants` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `variant_id` varchar(64) NOT NULL,
  `usku_id` varchar(64) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `usku_id` (`variant_id`, `usku_id`),
  CONSTRAINT `variants_ibfk_1` FOREIGN KEY (`usku_id`) REFERENCES `usku_record` (`usku_id`) ON DELETE CASCADE
);