-- One MySQL database (schema) per Medallion layer, plus one for data-quality metadata.
-- ${BRONZE} / ${SILVER} / ${GOLD} / ${DQ} are replaced by src/db.py using the DB_PREFIX setting
-- (default prefix "hotel_" -> hotel_bronze, hotel_silver, hotel_gold, hotel_dq).

CREATE DATABASE IF NOT EXISTS `${BRONZE}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS `${SILVER}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS `${GOLD}`   CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS `${DQ}`     CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
