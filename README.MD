Comparison of pipeline options:

COPY INTO

- 

Databricks Delta Live Tables
- Simple declarative pipelines
- Great for internal usage and analytics
- Limitations with sharing
- Newer feature does introduce physical [Delta tables as sink](https://www.databricks.com/blog/introducing-dlt-sink-api-write-pipelines-kafka-and-external-delta-tables), but I have not used and did not feel it appropriate for the limited time to develop this project



File Structure:
millimandatalake <br />
|-- bronze <br />
|---- _checkpoint <br />
|---- ccda <br />
|---- claims <br />
|---- rx <br />
|-- silver <br />
|-- gold