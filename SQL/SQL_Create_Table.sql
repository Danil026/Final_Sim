Создание таблицы sale_market (DDl)
CREATE TABLE public.sale_market (
	client_id int4 NULL,
	gender varchar(10) NULL,
	purchase_datetime date NULL,
	purchase_time_as_seconds_from_midnight int4 NULL,
	product_id int4 NULL,
	quantity int4 NULL,
	price_per_item numeric NULL,
	discount_per_item numeric NULL,
	total_price numeric NULL,
	CONSTRAINT sale_market_unique UNIQUE (client_id, gender, purchase_datetime, purchase_time_as_seconds_from_midnight, product_id, quantity, price_per_item, discount_per_item, total_price)
);