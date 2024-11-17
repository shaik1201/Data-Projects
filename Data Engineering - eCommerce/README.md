# Data Engineering Project: Ecommerce Data Simulation and Processing Pipeline

This project implements a real-time e-commerce data processing pipeline using modern data engineering tools and technologies. It simulates e-commerce data, processes it through various stages, and stores it in a relational database, all containerized using Docker.

## Project Architecture

The system architecture includes the following components:

- **Data Simulator**: Simulates ecommerce data and sends it to Kafka (using Flask).
- **Kafka**: Streams the simulated data.
- **Spark**: Processes the streamed data.
- **MySQL**: Stores the processed data.

![System Architecture](https://github.com/shaik1201/Data-Projects/blob/main/Data%20Engineering%20-%20eCommerce/Architecture.png)

## Requirements

To run this project locally, you need to have the following installed:

- Docker
- Docker Compose

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
```

### 2. Environment Setup

The project uses Docker Compose to manage all services. No additional local installations are required.

### 3. Build and Launch

Start all services using Docker Compose:

```bash
docker-compose up --build -d
```

This command will:
- Build custom images for each service
- Start all containers in detached mode
- Set up necessary networking between containers
- Initialize the MySQL database

## Monitoring and Verification

### Check Kafka Streams

View incoming messages in Kafka:

```bash
docker exec -it <kafka-container-id> kafka-console-consumer \
    --bootstrap-server localhost:29092 \
    --topic ecommerce-transactions \
    --from-beginning
```

### Monitor Spark Processing

View Spark processing outputs:

```bash
docker logs -f <spark-container-id>
```

Example output:
```
window_start       |window_end         |category |total_sales |avg_price|total_quantity|num_transactions
2024-11-17 18:29:13|2024-11-17 18:30:13|home     |734.84      |367.42   |2            |1
2024-11-17 18:29:13|2024-11-17 18:30:13|books    |277.44      |69.36    |4            |1
```

### Query MySQL Database

Access the MySQL shell:

```bash
docker exec -it <mysql-container-id> mysql -u spark -p ecommerce
# Password: spark123
```

View processed data:
```sql
USE ecommerce;
SHOW TABLES;
SELECT * FROM sales_analytics;
```

## Implementation Details

### Data Simulator
- Generates realistic e-commerce transaction data
- Simulates various product categories, prices, and quantities
- Streams data to Kafka in real-time

### Kafka Configuration
- Single broker setup for development
- Topic: `ecommerce-transactions`
- Configurable retention and partition settings

### Spark Streaming
- Processes data in micro-batches
- Calculates key metrics:
  - Total sales per category
  - Average price per transaction
  - Transaction counts
  - Quantity sold
- Implements windowed aggregations

### MySQL Schema
```sql
CREATE TABLE IF NOT EXISTS sales_analytics (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    category VARCHAR(255),
    total_sales DOUBLE,
    avg_price DOUBLE,
    total_quantity BIGINT,
    num_transactions INT,
    PRIMARY KEY (window_start, window_end, category)
);
```

## Troubleshooting

1. **Containers not starting:**
   ```bash
   docker-compose logs <service-name>
   ```

2. **No data in MySQL:**
   - Check Kafka consumer status
   - Verify Spark processing logs
   - Ensure MySQL container is healthy

3. **Spark processing errors:**
   - Check for schema mismatches
   - Verify Kafka connectivity
   - Review resource allocations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.