CREATE TABLE public.stock_bar (
	"timestamp" timestamptz NOT NULL,
	symbol bpchar(8) NOT NULL,
	time_scale bpchar(8) NOT NULL,
	"open" numeric NOT NULL,
	high numeric NOT NULL,
	low numeric NOT NULL,
	"close" numeric NOT NULL,
	volume int4 NOT NULL,
	trade_count int4 NOT NULL,
	vwap numeric NOT NULL,
	CONSTRAINT stock_bar_pk PRIMARY KEY ("timestamp", symbol, time_scale)
);
CREATE INDEX stock_bar_symbol_idx ON public.stock_bar USING btree (symbol, time_scale);
