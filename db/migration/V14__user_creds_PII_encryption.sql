ALTER TABLE `user_creds`
  MODIFY COLUMN `user_name` varchar(255) NOT NULL,
  MODIFY COLUMN `phone_number` varchar(255) NOT NULL,
  MODIFY COLUMN `user_email` varchar(512) NOT NULL,
  MODIFY COLUMN `user_designation` varchar(255) NOT NULL,
  MODIFY COLUMN `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  ADD COLUMN `email_lookup` varchar(64) NOT NULL AFTER `user_email`,
  ADD COLUMN `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  ADD UNIQUE KEY `email_lookup` (`email_lookup`);