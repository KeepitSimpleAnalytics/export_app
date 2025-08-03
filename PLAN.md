# Project Plan: Air-gapped Data Utility (ADU)

## Overall Goal
To develop a containerized web application for secure data migration and conversion in air-gapped environments, featuring a user-friendly web interface, robust job history, and detailed error logging.

## Remaining Steps

### 1. Core Data Processing Implementation
*   **Actual Data Export Logic:** Implement the core data extraction and conversion using Polars, DuckDB, and database connectors (`psycopg2-binary`, `vertica-python`).
    *   Connect to source databases using provided credentials.
    *   Discover schemas and tables (if not explicitly provided).
    *   Perform parallel data export for selected tables.
    *   Convert data to Parquet format.
    *   Handle data writing to the designated output location (e.g., mounted volume).
*   **Pandera Integration:** Integrate `pandera` for data validation during the conversion process.

### 2. Enhanced User Interface (UI/UX)
*   **Job Details Page:** Fully populate the Job Details page (`/job/<job_id>`) with:
    *   Full job configuration display.
    *   Summary of export (total tables, successful, failed).
    *   Status of each individual table within the job.
    *   Link to the worker's log file for the job.
    *   Improved display of captured errors (formatted tracebacks, context).
*   **Dashboard Enhancements:** Add real-time feedback or progress indicators for running jobs on the main dashboard.
*   **User Authentication/Authorization:** Implement a proper authentication system (e.g., Flask-Login) if required for multi-user environments.

### 3. Robust Logging and Monitoring
*   **Structured Logging:** Ensure Python's standard `logging` module is used with structured formatting.
*   **Sensitive Data Redaction:** Implement redaction for sensitive information (e.g., passwords, API keys) in logs.
*   **Log File Access:** Provide a secure way to access worker log files from the web interface.

### 4. Containerization Refinements
*   **Multi-service Docker Compose:** Consider using Docker Compose to orchestrate the Flask web application and the worker as separate services, if needed for scalability or resource management.
*   **Volume Management:** Ensure proper Docker volume setup for persistent storage of the SQLite database and exported data.

### 5. Testing and Verification
*   **Unit Tests:** Write unit tests for critical components (database interactions, data processing logic, error handling).
*   **Integration Tests:** Develop integration tests to ensure the frontend, backend, and worker communicate correctly.
*   **End-to-End Tests:** Implement end-to-end tests to simulate user workflows.
*   **Build and Linting:** Integrate project-specific build, linting, and type-checking commands into the development workflow.

### 6. Deployment Considerations
*   **Production-Ready Configuration:** Adjust Flask and Gunicorn (or similar WSGI server) settings for production deployment.
*   **Environment Variables:** Externalize sensitive configurations using environment variables.
