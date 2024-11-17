# Data Engineering Project: Ecommerce Data Simulation and Processing Pipeline

This project implements a real-time e-commerce data processing pipeline using modern data engineering tools and technologies. It simulates e-commerce data, processes it through various stages, and stores it in a relational database, all containerized using Docker.

## Project Architecture

The system architecture includes the following components:

- **Data Simulator**: Simulates ecommerce data and sends it to Kafka (using Flask).
- **Kafka**: Streams the simulated data.
- **Spark**: Processes the streamed data.
- **MySQL**: Stores the processed data.

![System Architecture](path_to_your_system_architecture_image.png)

## Folder Structure

The project folder structure is as follows:

├── data-simulator/
│   ├── app/
│   │   ├── __init__.py
│   │   ├── requirements.txt
│   │   ├── simulator.py
│   │   └── test_connection.py
│   └── Dockerfile
├── kafka/
│   ├── Dockerfile
│   └── producer.py
├── mysql/
│   └── init.sql
├── spark/
│   ├── Dockerfile
│   ├── log4j.properties
│   ├── requirements.txt
│   └── spark_processor.py
└── docker-compose.yml



## Requirements

To run this project locally, you need to have the following installed:

- Docker
- Docker Compose

## Getting Started

Follow these steps to get the project up and running on your local machine.

### Step 1: Clone the Repository

```bash
git clone https://github.com/yourusername/your-repo-name.git
cd your-repo-name
```

### Step 2: Build and Start the Docker Containers

Run the following command to build and start all containers using Docker Compose:
```bash
docker-compose up --build -d
```
This command will start the following services:

Data Simulator: Generates simulated ecommerce data and streams it to Kafka.
Kafka: Kafka instance for streaming the data.
Spark: Spark job that consumes the Kafka stream, processes it, and stores the results in MySQL.
MySQL: MySQL database where processed data is stored.
The services will run in the background, and you will be able to track logs to monitor the process.

