# Docker Airflow Metrics Data Pipeline

## Docker-Airflow Architecture

<img src="diagrams\docker-airflow-architecture-314mini.drawio.svg" alt="Docker Airflow Architecture">

### A Complete Task Lifecycle<br>
**0. Initialization**
- The initialization container first checks system resources, then connects to PostgreSQL to run database migrations, creating all necessary tables and schemas. It also creates default connections and the admin user. This is a prerequisite for all other services. <br>

**1. Scheduler --> ./dags volume**
- The Scheduler constantly scans the DAGs folder to parse DAG files and detect changes.

**2. Webserver --> ./dags volume**
- The Webserver reads DAG files to display their structure and code in the UI.

**3. Scheduler --> Postgres**
- After parsing DAGs, the Scheduler updates the database with information about DAG structure, schedule intervals, and any changes it detects. 
- It also reads from the Database to determine  which tasks are ready to be scheduled (confirms that taks X's dependencies are met).

**4. Scheduler --> Redis**
- When the Scheduler identifies a task that needs to run, it sends a message to Redis (the Celery broker) with the task information. 
- The Scheduler writes a message to Redis saying "task X is ready to run."
- Akin to putting a work order in a shared queue.

**5. Redis --> Celery Worker**
- After The Scheduler writes a message to Redis saying "task X is ready to run", a Celery Worker pulls this message from Redis.

**6. Celery Worker --> ./dags volume**
- The worker reads the DAG definition to understand exactly what code to execute.
  
**7. Celery Worker --> Postgres** 
- The Worker updates PostgreSQL to mark the task as "running."

**8. Celery Worker --> ./logs**
- The Worker writes to the logs volume as the task is executed by the Worker.

**9. Celery Worker --> Postgres** 
- Upon completion, the Worker updates PostgreSQL to mark the task as "success".

**10. Webserver --> Postgres**
- The Webserver reads the status from PostgreSQL and displays it in the UI.

**11. Webserver --> ./logs volume**
- The Webserver reads from the logs volume to display task logs in the web UI. 


## DAGs Execution Order

<img src="diagrams\dag-execution-order-mini.drawio.svg" alt="DAGs Execution Order">
