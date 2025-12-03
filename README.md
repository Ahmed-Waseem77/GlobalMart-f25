![GitHub](https://img.shields.io/badge/Release-PROD-green.svg)
![GitHub](https://img.shields.io/badge/Version-0.0.1-lightgrey.svg)
![GitHub](https://img.shields.io/badge/License-MIT-blue.svg)

| Container | Image | Tag | Accessible | 
|-|-|-|-|
| zookeeper | wurstmeister/zookeeper | latest | 172.25.0.11:2181 |
| kafka1 | wurstmeister/kafka | 2.12-2.2.0 | 172.25.0.12:9092 (port 8080 for JMX metrics) |
| kafka1 | wurstmeister/kafka | 2.12-2.2.0 | 172.25.0.13:9092 (port 8080 for JMX metrics) |
| kafka_manager | hlebalbau/kafka_manager | 1.3.3.18 | 172.25.0.14:9000 |
| prometheus | prom/prometheus | v2.8.1 | 172.25.0.15:9090 |
| grafana | grafana/grafana | 6.1.1 | 172.25.0.16:3000 |
| spark-master | bde2020/spark-master | 3.3.0-hadoop3.3 | 172.25.0.20:8081 (UI), :7077 (Master) |
| spark-worker | bde2020/spark-worker | 3.3.0-hadoop3.3 | 172.25.0.21 |
| namenode | bde2020/hadoop-namenode | 2.0.0-hadoop3.2.1-java8 | 172.25.0.22:9870 (UI), :8020 (IPC) |
| datanode | bde2020/hadoop-datanode | 2.0.0-hadoop3.2.1-java8 | 172.25.0.23 |
| hbase-master | bde2020/hbase-master | 1.0.0-hbase1.2.6 | 172.25.0.24:16010 (UI), :16000 (IPC) |
| hbase-regionserver | bde2020/hbase-regionserver | 1.0.0-hbase1.2.6 | 172.25.0.25 |
| Jupyter | jupyter/pyspark-notebook | latest | 172.25.0.19 | :4040, :8888 (UI) |

# Quickstart

The easiest way to understand the setup is by diving into it and interacting with it.

## Running Docker Compose

To run docker compose simply run the following command in the current folder:

```
docker-compose up -d
```

This will run deattached. If you want to see the logs, you can run:

```
docker-compose logs -f -t --tail=10
```

To see the memory and CPU usage (which comes in handy to ensure docker has enough memory) use:

```
docker stats
```

## Operations

### Jupyter Notebook
Just open a browser and visit localhost:8888, password `admin`

### Monitor Kafka

We can now use the kafka manager to dive into the current kafka setup.

#### Setup Kafka Manager

To set up kafka manager we need to configure it. In order to do this, access http://172.25.0.14:9000/addCluster and fill up the following two fields:

* Cluster name: Kafka
* Zookeeper hosts: 172.25.0.11:2181

Optionally:
* You can tick the following;
    * Enable JMX Polling
    * Poll consumer information

#### Visualise metrics in Grafana

Finally, you can access the default kafka dashboard in Grafana (username is "admin" and password is "password") by going to http://172.25.0.16:3000/d/xyAGlzgWz/kafka?orgId=1

![](images/grafanakafka.jpg)

Monitoring is set up by promethues source for both kafka and spark containers.


## Scaling Services
You can scale the Spark workers, HDFS datanodes, and HBase regionservers using the `--scale` flag:

```bash
docker-compose up -d --scale spark-worker=3 --scale hbase-regionserver=3
```

>[!NOTE]
> - **spark-worker**: Scales the number of Spark worker nodes.
> - **hbase-regionserver**: Scales the number of HBase region servers.

>[!WARNING]
> To Scale datanodes you need to scale them manually by adding datanode nodes with a fixed IP in `docker-compose.yml`
> This is due to dynamically scaled datanodes not able to communicate with the namenode, which fails our HBase.
> _We suspect this is due to them not being given an IP but more investigation is needed_


## Running GlobalMart

First Navigate to the Jupyter Console on the browser on `localhost:8888`

Install global dependencies in the node with 

```
pip install -r requirements.txt
```

Run the data generation script 

```
python stream.py
```
 
In a second terminal initialize the HBASE schema and run the ETL Flow

```
python spark_etl_flow.py
```

In a third terminal you can run any of the validation script to make sure the flow is working.

## Data Generation Script

There are multiple Patterns that are coded in the data stream to evoke the sense of real data.

### Users
- **Demographics**: Users are generated with a weighted distribution across 5 countries: Egypt (Dominant), Eswatini, Timor-Leste, Cambodia, and Federated States of Micronesia.
- **Age**: Ages follow a normal distribution (mean=26, std_dev=10), clipped to the range [18, 90].
- **Email**: Randomly assigned from common domains (gmail, yahoo, etc.).

### Products
- **Categories**: Electronics, Clothing, Home, Books, Sports, Beauty, Toys.
- **Pricing & Inventory**:
    - **Electronics**: High price ($100-$3000), Low inventory.
    - **Books**: Low price ($5-$50), High inventory.
    - **Clothing**: Mid-range price ($10-$200), Mid-range inventory.
    - **Others**: General price ($10-$500) and inventory ranges.
- **Ratings**: Normally distributed (mean=4.2, std_dev=0.8), clipped to [1.0, 5.0].

### Transactions
- **Buying Patterns**: Users have country-specific category preferences (e.g., users from Egypt prefer Electronics and Clothing).
- **Product Selection**: 50% chance to select products matching the user's country preference.
- **Payment Methods**: Randomly selected from Credit Card, PayPal, Debit Card, Apple Pay, Google Pay.

### Sessions
- **Activity**: Each session contains 1-10 events.
- **Event Types**: ADD_TO_CART, REMOVE_FROM_CART, CLEAR_CART.

### Data Quality (Chaos Engineering)
- **Null Injection**:
    - **Critical IDs** (product_id, session_id): 1% chance of being NULL.
    - **General Fields**: 0.5% chance of being NULL.
    - **Key Fields** (transaction_id, user_id, timestamp): Never NULL.
##
