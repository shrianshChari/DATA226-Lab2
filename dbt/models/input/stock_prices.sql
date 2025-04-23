SELECT
		date,
		stock_symbol,
		close
FROM {{ source("raw", "stock_prices") }}
