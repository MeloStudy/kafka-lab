# Infrastructure Requirements: [Lab Name]

Describe the infrastructure required to run this lab.

- **Option A: Basic KRaft Broker** 
  - Use `docs/templates/infra/docker-compose-basic.yml`
- **Option B: Schema Registry included**
  - Use `docs/templates/infra/docker-compose-schema.yml`
- **Option C: Testcontainers (Service Connections)**
  - No `docker-compose.yml` needed. Managed automatically by Spring Boot in Java.
