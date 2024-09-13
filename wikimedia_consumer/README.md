# Structured Streaming Demos

## Steps to Start

1. Ensure you have built the `gc-pyspark-kafka:latest` image. ([See instructions.](https://docs.google.com/document/d/11_wF1kem1rGynw7fJiPJ2J8BWdrts1JTQuCEX65vIGs/preview))
2. Start the appropriate producer.
3. Run `docker compose up`.
4. Locate and open the URL in the logs. Should look something like: http://127.0.0.1:8888/lab?token=11125465301bd5de1f36183cf093a1a79a313eb3aa3bba1e with a unique token.
5. In the JupyterLabs environment that loads, open a terminal.
6. Ensure that the correct port number for your Docker Kafka is set in `BOOTSTRAP_SERVERS`.
7. `cd` to the `work` directory and run the desired file using Python. `wikimedia_events.py`
8. Afterward, run `python combine_csvs.py output/wikimedia_events.csv` to combine the folder of CSVs into one that can be loaded with Pandas or another visualization tool.