# README file content#  E-commerce Sales Data Pipeline

A scalable data engineering solution built on **Databricks + PySpark** for ingesting, enriching, and aggregating E-commerce sales data.

---

##  Project Structure


---

##  Data Sources

- `orders.csv`
- `customers.csv`
- `products.csv`
- `transactions.csv`

Place files in `/FileStore/tables/` for testing, or mount them via Unity Catalog for production.

---

##  Features

-  Bronze, Silver, Gold layer using Delta Lake
-  Join orders, customers, transactions, products
-  Calculate profit and build summary tables
-  Unit tests with `pytest` and `chispa`
-  100% test coverage + HTML report
-  GitHub Actions + Databricks CLI automation

---

##  Run Tests & Coverage

```bash
pip install -r requirements.txt
pip install -r dev-requirements.txt

pytest --cov=ecommerce_pipeline --cov-report=html



## DataBricks Deployment

databricks workspace import_dir notebooks /Workspace/ecommerce_pipeline --overwrite
databricks jobs create --json-file databricks_job.json

databricks jobs run-now --job-id <your-job-id>

.github/workflows/databricks-pipeline.yml

##Assumptions
Input CSVs are clean and header-based

Dates are parseable with format %Y-%m-%d

Transactions contain valid sales and cost values

Author
Sangam Choubey
