# 🛒 E-commerce Microservices

An e-commerce system built with **microservices architecture**, asynchronous communication via **Apache Kafka**, service integration, and containerized infrastructure with **Docker**.  
The project was developed as part of the **Microservices Architecture with Spring Boot and Apache Kafka** course exercise, to demonstrate **best practices in distributed architecture**, scalability, and decoupling.

---

## 🏗 Architecture

The system follows the **event-driven** pattern with independent microservices, each responsible for a specific domain.  
Communication between services occurs via **Apache Kafka** (events) and **REST APIs** (direct queries).

**Overview:**
[orders-service] → Feign → [products-service] → Feign → [customers-service] ↓ Webhooks ↔ [simulated payment gateway] → Kafka ↓ [invoicing-service] → MinIO (reports) → Kafka → [logistic-service] → tracking code generation

---


📝 **Flow Explanation:**

1. `orders-service` queries `products-service` and `customers-service` via **Feign Clients** to validate the order.
2. A webhook sends data to the **simulated payment gateway**, which processes the payment and publishes events to **Kafka**.
3. `invoicing-service` consumes payment events and generates invoices, storing reports in **MinIO**.
4. Invoice events are also sent via Kafka to `logistic-service`, which processes logistics and issues the **tracking code**.

---

## 🛠 Technologies

- **Java 21** + **Spring Boot**
- **Apache Kafka** (messaging and asynchronous communication)
- **Docker & Docker Compose**
- **Spring Data JPA** (PostgreSQL / MongoDB)
- **Feign Client** (service integration)
- **Webhooks** (external notifications)
- **MinIO** (S3-compatible object storage)
- **Jasper Reports** (PDF/Excel reports)
- **JWT / OAuth2** (authentication and authorization)

---

## 📦 Services

| Service                | Description                          |
|-------------------------|--------------------------------------|
| **customers-service**   | Customer registration and management |
| **products-service**    | Product catalog and information      |
| **orders-service**      | Order creation and management        |
| **logistic-service**    | Logistics and delivery processing    |
| **invoicing-service**   | Billing and invoice generation       |
| **infrastructure-service** | Support services and integrations |

---

## 🔄 Communication Flow

1. **Customer creates order** → `orders-service`
2. Simulated payment webhook notifies `orders-service`
3. `orders-service` publishes **orders-paid** event to Kafka
4. **invoicing-service** consumes event and generates invoice (Jasper Reports)
5. Report is stored in **MinIO**
6. **logistic-service** consumes event and starts delivery process, generating tracking code

---

## 🎓 Course Learnings
- **Webhooks**: integration and communication between external systems via HTTP
- **Cloud Buckets with MinIO**: storage, retrieval, and management of files compatible with Amazon S3
- **Jasper Reports**: creation of dynamic, professional reports integrated into microservices

**Key Takeaways:**
- Master microservices architecture and communication in distributed systems
- Implement event flows with Kafka and partitioned topics
- Manage independent databases and files in Cloud Buckets

---

## 🚀 How to Run

```bash
# 1. Clone repository
git clone https://github.com/CaiquePirs/ecommerce-microservices.git
cd ecommerce-microservices

# 2. Start infrastructure (Kafka, MinIO, DBs)
docker-compose up -d

# 3. Run each service
cd customers-service && mvn spring-boot:run
cd ../products-service && mvn spring-boot:run
# ... repeat for other services
```

## 📡 Kafka Events

| Event           | Producer           | Consumer(s)                        | Description       |
|-----------------|------------------|-----------------------------------|-----------------|
| OrderCreated    | orders-service    | logistic-service, invoicing-service | Order created    |
| OrderShipped    | logistic-service  | orders-service                     | Order shipped    |
| InvoiceGenerated| invoicing-service | orders-service                     | Invoice generated|


