<div>

# 🛒 Event-Driven E-commerce — Microservices Platform

**A distributed e-commerce backend built on an event-driven architecture with Apache Kafka, Spring Boot, and containerized infrastructure.**

From order placement to invoice generation and shipment tracking — fully asynchronous, decoupled, and independently deployable.

![Java](https://img.shields.io/badge/Java-21-ED8B00?style=flat&logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5-6DB33F?style=flat&logo=springboot&logoColor=white)
![Spring Cloud](https://img.shields.io/badge/Spring%20Cloud-OpenFeign-6DB33F?style=flat&logo=spring&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Event%20Streaming-231F20?style=flat&logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?style=flat&logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-Object%20Storage-C72E49?style=flat&logo=minio&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)

</div>

---

## Overview

This project models the core of an online store as a set of **independent, single-responsibility microservices** that collaborate through a mix of:

- **Synchronous communication (REST / OpenFeign)** — used when an operation needs an immediate answer (e.g. validating a customer or reserving product stock while an order is being created).
- **Asynchronous communication (Apache Kafka)** — used to propagate *facts that already happened* (an order was paid, invoiced, or shipped) so downstream services can react on their own time, without coupling.

The result is a system where **payment, invoicing, and logistics evolve and scale independently**, and where a slow or offline downstream service never blocks the checkout path.

> Developed as the capstone of a **Microservices Architecture with Spring Boot & Apache Kafka** course, with a focus on real-world distributed-systems practices: decoupling, eventual consistency, object storage, and report generation.

---

## System Architecture

Each service owns its data and its lifecycle. Customers and products are queried synchronously during checkout; everything after payment is driven by events.

```text
                            ┌───────────────┐
                            │    CLIENT     │
                            └───────┬───────┘
                                    │  POST /api/orders
                                    ▼
      ════════════════  SYNCHRONOUS · REST / Feign  ════════════════

    validate customer   ┌──────────────┐   reserve stock   ┌──────────────┐
         ┌─────────────▶│  customers   │        ┌──────────▶│   products   │
         │              │    :8082     │        │           │    :8081     │
         │              └──────────────┘        │           └──────────────┘
    ┌────┴──────────────────────────────────────┴────┐
    │             orders-service · :8083              │◀─────┐
    └───────────────────────┬─────────────────────────┘      │ webhook
                            │  process payment                │ (payment ok)
                            ▼                                 │
                   ┌────────────────────┐                     │
                   │   Payment Gateway  │─────────────────────┘
                   │     (simulated)    │
                   └─────────┬──────────┘
                             │  publish: ecommerce.orders-paid
                             ▼
      ═══════════════  APACHE KAFKA · event bus  ═══════════════
              │                                         ▲    │
   orders-paid│                           orders-shipped│    │orders-invoiced
              ▼                                          │    ▼
     ┌──────────────────┐   orders-invoiced   ┌──────────────────┐
     │ invoicing · :8084│────────────────────▶│ logistic · :8085 │
     └────────┬─────────┘                     └──────────────────┘
              │  store PDF invoice
              ▼
     ┌──────────────────┐
     │  MinIO · storage │
     └──────────────────┘
```

---

## End-to-End Order Flow

The full lifecycle of an order — from checkout to a shippable tracking code — split into a **synchronous checkout phase** and an **asynchronous fulfillment phase**.

**Pipeline at a glance:**

> `orders` ➜ **`orders-paid`** ➜ `invoicing` ➜ **`orders-invoiced`** ➜ `logistic` ➜ **`orders-shipped`** ➜ `orders`

**Phase 1 · Checkout — _synchronous_**

1. The **client** sends `POST /api/orders`.
2. **orders-service** validates the customer via Feign → **customers-service**.
3. It reserves and decrements stock via Feign → **products-service**.
4. It processes payment through a payment **Strategy** (`CREDIT` / `DEBIT` / `PIX` / `BOLETO`), receives a `paymentKey`, and the order becomes **`PLACED`**.

**Phase 2 · Payment confirmation — _webhook_**

1. The payment gateway calls back `POST /api/orders/callback-payment`; the order becomes **`PAID`**.
2. **orders-service** publishes the event **`ecommerce.orders-paid`** to Kafka.

**Phase 3 · Invoicing — _asynchronous_**

1. **invoicing-service** consumes `orders-paid` and generates a **PDF invoice** with JasperReports.
2. The invoice is uploaded to **MinIO** object storage.
3. It publishes **`ecommerce.orders-invoiced`** to Kafka.

**Phase 4 · Logistics — _asynchronous_**

1. **logistic-service** consumes `orders-invoiced` and generates a **tracking code**.
2. It publishes **`ecommerce.orders-shipped`** to Kafka.

**Phase 5 · Status reconciliation**

1. **orders-service** consumes `orders-invoiced` and `orders-shipped`, advancing the order status: **`INVOICED`** ➜ **`SHIPPED`**.

---

## Services

| Service | Port | Persistence | Communication | Responsibility |
|---|:---:|:---:|:---:|---|
| **`customers-service`** | `8082` | PostgreSQL | REST | Customer registration, profile & address management, deactivation. |
| **`products-service`** | `8081` | PostgreSQL | REST | Product catalog, categories, and stock control (reserve / replenish). |
| **`orders-service`** | `8083` | PostgreSQL | REST · Feign · Kafka | Order orchestration, payment processing, and status lifecycle. **The heart of the system.** |
| **`invoicing-service`** | `8084` | MinIO | Kafka | Generates PDF invoices (JasperReports) and stores them in object storage. |
| **`logistic-service`** | `8085` | — | Kafka | Produces shipment tracking codes and emits shipping events. |
| **`infrastructure-service`** | — | — | — | Docker Compose definitions for Kafka, PostgreSQL, and MinIO. |

---

## Kafka Topics

Events are named after **facts that already happened** (past tense) — the backbone of the event-driven design.

| Topic | Producer | Consumer(s) | Meaning |
|---|---|---|---|
| `ecommerce.orders-paid` | `orders-service` | `invoicing-service` | Payment confirmed — trigger invoicing. |
| `ecommerce.orders-invoiced` | `invoicing-service` | `logistic-service`, `orders-service` | Invoice issued — trigger shipping & update order. |
| `ecommerce.orders-shipped` | `logistic-service` | `orders-service` | Shipment dispatched — order gains a tracking code. |

**Order status lifecycle:** `PLACED → PAID → INVOICED → SHIPPED` &nbsp;·&nbsp; failure path: `PAYMENT_ERROR`

---

## Tech Stack

| Category | Technologies |
|---|---|
| **Language & Runtime** | Java 21 |
| **Framework** | Spring Boot 3.5, Spring Web, Spring Data JPA, Spring Kafka |
| **Service Integration** | Spring Cloud OpenFeign (declarative REST clients) |
| **Messaging** | Apache Kafka (Confluent images) + Kafka UI |
| **Databases** | PostgreSQL 16 (per-service database) + pgAdmin |
| **Object Storage** | MinIO (S3-compatible) for invoice PDFs |
| **Reporting** | JasperReports 7 (dynamic PDF invoice generation) |
| **Mapping & Boilerplate** | MapStruct, Lombok |
| **Infrastructure** | Docker & Docker Compose |
| **Build** | Maven |

---

## Design Patterns & Practices

This project is intentionally structured to demonstrate clean, production-minded architecture:

- **Event-Driven Architecture** — services react to domain events instead of calling each other directly, enabling temporal decoupling and independent scaling.
- **Database-per-Service** — each microservice owns its schema; no shared database, no hidden coupling.
- **Strategy + Factory** — payment methods (`CREDIT`, `DEBIT`, `PIX`, `BOLETO`) are pluggable strategies resolved through a `PaymentMethodFactory`.
- **Gateway Pattern** — the payment integration is hidden behind a `PaymentGateway` contract, keeping the domain independent of the provider.
- **Use Case–oriented design** — business rules live in focused, single-purpose use cases (`CreateOrderUseCase`, `AddStockProductUseCase`, …) rather than bloated services.
- **DTO / Representation boundaries** — API contracts and Kafka payloads are decoupled from JPA entities.
- **Centralized error handling** — each service exposes a `@RestControllerAdvice` with structured error responses.

---

## Getting Started

### Prerequisites

- **JDK 21+**
- **Maven 3.9+**
- **Docker** & **Docker Compose**

### 1. Clone the repository

```bash
git clone https://github.com/CaiquePirs/events-driven-microservices.git
cd events-driven-microservices
```

### 2. Start the infrastructure

Infrastructure is split into three independent Compose stacks under `infrastructure-service/`. Start each one:

```bash
# Apache Kafka + Zookeeper + Kafka UI (:8090)
docker compose -f infrastructure-service/broker/docker-compose.yml up -d

# PostgreSQL (:5432) + pgAdmin (:5050)
docker compose -f infrastructure-service/database/docker-compose.yml up -d

# MinIO object storage — API (:9000) + Console (:9001)
docker compose -f infrastructure-service/bucket/docker-compose.yml up -d
```

> 💡 On older Docker versions use the hyphenated `docker-compose` command.

### 3. Prepare the databases

The services expect the databases `orders-db`, `customers-db`, and `product_db` (see each service's `application.yml`). Create them via pgAdmin (`localhost:5050`, `admin@admin.com` / `admin123`) or `psql`. Schemas are auto-generated by Hibernate (`ddl-auto: update`) on first run.

### 4. Run the services

Each service is a standalone Spring Boot app. In separate terminals:

```bash
cd products-service   && mvn spring-boot:run   # :8081
cd customers-service  && mvn spring-boot:run   # :8082
cd orders-service     && mvn spring-boot:run   # :8083
cd invoicing-service  && mvn spring-boot:run   # :8084
cd logistic-service   && mvn spring-boot:run   # :8085
```

### 5. Try it out

Create a product, register a customer, then place an order against `orders-service` — watch the events flow through **Kafka UI** (`localhost:8090`) and the generated invoice appear in the **MinIO console** (`localhost:9001`).

---

## API Reference

### Customers — `http://localhost:8082/api/customers`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/` | Register a new customer |
| `GET` | `/{id}` | Fetch a customer by id |
| `GET` | `/{id}/orders` | List a customer's orders (Feign → orders-service) |
| `PUT` | `/{id}` | Update customer data |
| `DELETE` | `/{id}` | Deactivate a customer |

### Products — `http://localhost:8081/api/products`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/` | Create a product |
| `GET` | `/{id}` | Fetch a product by id |
| `PUT` | `/{id}?stock={n}` | Consume `n` units of stock |
| `PUT` | `/{id}/update-stock?stock={n}` | Replenish stock by `n` units |
| `DELETE` | `/{id}` | Delete a product |

### Orders — `http://localhost:8083/api/orders`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/` | Create an order (validates customer & stock, processes payment) |
| `GET` | `/{id}` | Fetch an order by id |
| `GET` | `/{id}/customer` | List all orders for a customer |
| `PUT` | `/update-payment` | Update payment details of an order |
| `POST` | `/callback-payment` | **Payment gateway webhook** — confirms payment (`apiKey` header) |

### Invoicing — `http://localhost:8084/api/invoicing/bucket`

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/?file=` | Upload a file to object storage |
| `GET` | `/?fileName=` | Get a temporary redirect URL to a stored invoice |

---

## Ports & Endpoints Reference

| Component | URL | Credentials |
|---|---|---|
| products-service | `http://localhost:8081` | — |
| customers-service | `http://localhost:8082` | — |
| orders-service | `http://localhost:8083` | — |
| invoicing-service | `http://localhost:8084` | — |
| logistic-service | `http://localhost:8085` | — |
| Kafka (host listener) | `localhost:29092` | — |
| **Kafka UI** | `http://localhost:8090` | — |
| PostgreSQL | `localhost:5432` | `admin` / `admin123` |
| **pgAdmin** | `http://localhost:5050` | `admin@admin.com` / `admin123` |
| **MinIO Console** | `http://localhost:9001` | `admin` / `admin123` |
| MinIO API | `http://localhost:9000` | `admin` / `admin123` |

> ⚠️ The credentials above are development defaults only. **Never** ship them to production.

---
