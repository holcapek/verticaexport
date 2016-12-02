COPY stock_items FROM LOCAL 'stock-items.csv'
	DELIMITER ','
	ENCLOSED '"'
	REJECTED DATA 'stock-items.csv.rejected'
	EXCEPTIONS 'stock-items.csv.errors';
COPY tv_spots    FROM LOCAL 'TV-Spots.csv'
	DELIMITER ','
	ENCLOSED '"'
	REJECTED DATA 'TV-Spots.csv.rejected'
	EXCEPTIONS 'TV-Spots.csv.errors';
