# File-Converter-Project
# Goal-of-Project
The objective of this project is to develop solutions based on the design provided. In this case, the source data was obtained in the form of CSV files from a MySQL DB.

To improve the efficiency of our data engineering pipelines, we need to convert these CSV files into JSON files, since JSON is better to use in downstream applications than CSV files. The scope of this project involves converting CSV files into JSON files.

## Features
- **Schema-Based Mapping**: Ensures correct column names and order.
- **Error Handling**: Logs missing files, malformed data, and schema mismatches.
- **Scalability**: Processes multiple datasets efficiently.

## Requirements
- Python 3.7+
- `pandas`
- `json`
- `logging`

Install dependencies with:
```bash
pip install -r requirements.txt
