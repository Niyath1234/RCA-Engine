# Trino Configuration

This directory contains the configuration files for Trino, a distributed SQL query engine.

## Configuration Files

- **config.properties**: Main Trino configuration (coordinator settings, memory limits, etc.)
- **node.properties**: Node-specific settings (environment, data directory)
- **jvm.config**: JVM settings for Trino
- **catalog/**: Catalog configurations for data sources

## Catalogs

### PostgreSQL (`postgres.properties`)
Connects to the PostgreSQL database service defined in docker-compose.yml.

**Note**: Update the `connection-password` in `postgres.properties` to match your `RCA_DB_PASSWORD` environment variable if you've changed it from the default.

### TPCH (`tpch.properties`)
TPC-H benchmark connector for testing and demos.

### TPCDS (`tpcds.properties`)
TPC-DS benchmark connector for testing and demos.

## Usage

### Starting Trino

```bash
docker-compose up trino
```

Or start with all services:
```bash
docker-compose up
```

### Accessing Trino

- **Web UI**: http://localhost:8081
- **JDBC URL**: `jdbc:trino://localhost:8081`
- **CLI**: Use the Trino CLI or any JDBC-compatible client

### Connecting to Trino

#### Using Trino CLI
```bash
docker exec -it rca-trino trino --server http://localhost:8080
```

#### Example Query
```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM postgres;
SELECT * FROM postgres.rca_engine.your_table LIMIT 10;
```

## Customization

To add more catalogs or modify settings:

1. Add catalog properties files in `catalog/` directory
2. Modify `config.properties` for server settings
3. Restart the Trino container: `docker-compose restart trino`

## Troubleshooting

- Check logs: `docker-compose logs trino`
- Verify health: `curl http://localhost:8081/v1/info`
- Ensure PostgreSQL is running if using the postgres catalog: `docker-compose ps postgres`
