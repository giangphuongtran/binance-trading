# Binance → Postgres → Streamlit labeler

.PHONY: up down schema download streamlit

up:
	docker compose up -d postgres

down:
	docker compose down

# Run after first up: create market schema and tables
schema:
	psql "$${POSTGRES_DSN}" -f sql/001_create_market_tables.sql

download:
	python scripts/download_btcusdt_futures_klines.py

streamlit:
	streamlit run app/streamlit_labeler.py
