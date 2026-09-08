# W1 Architecture

```mermaid
flowchart LR
    Raw["data/raw<br/>Taxi Parquet<br/>Weather CSV<br/>Air Quality CSV<br/>Zones CSV"]
    Config["configs/datasets.json<br/>source + contract + versions"]
    Reader["A: generic reader<br/>explicit raw schemas"]
    Prepare["B: prepare()<br/>transform + validate + deduplicate"]
    Accepted["accepted"]
    Rejected["rejected"]
    Writer["A: shared Delta writer<br/>write + read-back count check"]
    Standard["data/delta/standardized<br/>four Delta tables"]
    RejectTable["data/delta/rejected<br/>four Delta tables"]
    Metadata["data/delta/metadata/ingestion_runs<br/>run ID + counts + versions + status"]
    Integration["C: hourly aggregation<br/>left joins"]
    Integrated["data/delta/integrated<br/>integrated_taxi_trips"]

    Raw --> Reader
    Config --> Reader
    Config --> Prepare
    Reader --> Prepare
    Prepare --> Accepted
    Prepare --> Rejected
    Accepted --> Writer
    Rejected --> Writer
    Writer --> Standard
    Writer --> RejectTable
    Writer --> Metadata
    Standard --> Integration
    Integration --> Integrated
```

Role A owns the generic reader, configurable output root, shared Delta writer,
read-back validation, ingestion metadata and CLI. Role B owns source contracts,
standardization and data-quality rules. Role C consumes successful standardized
tables and uses the shared writer for integration and benchmark outputs.
