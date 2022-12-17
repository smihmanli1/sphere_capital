Pipeline for analyzing possibility of pairs trading in Borsa Istanbul. Runs through a single day of full order book and extracts minutely mid prices for each security.
Outputs correlations of prices of selected groups of securities with each other.

Run:
python3 bist50_pairs_trading_backtester2.py ../../../data/borsa_istanbul_equity_market/normalized_orders/ 2021-07-06