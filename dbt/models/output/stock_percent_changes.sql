-- Code adapted from https://stackoverflow.com/a/22764757
WITH x AS (
	SELECT * FROM {{ ref("stock_prices") }}
	), y AS (
	SELECT * FROM {{ ref("stock_prices") }}
)
SELECT x.date, x.stock_symbol, x.close, (((x.close / y.close) - 1) * 100) AS close_percent_change
FROM 
(
    SELECT a.Date AS aDate, MAX(b.Date) AS aPrevDate, a.stock_symbol AS aStockSymbol
    FROM x AS a
    INNER JOIN y AS b
    WHERE a.Date > b.Date AND a.stock_symbol = b.stock_symbol
    GROUP BY aDate, aStockSymbol
    ORDER BY aDate, aStockSymbol
) Sub1
INNER JOIN x ON Sub1.aDate = x.Date AND Sub1.aStockSymbol = x.stock_symbol
INNER JOIN y ON Sub1.aPrevDate = y.Date AND Sub1.aStockSymbol = y.stock_symbol
ORDER BY x.date DESC
