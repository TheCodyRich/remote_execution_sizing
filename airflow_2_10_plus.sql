-- The below queries will produce the sum duration of task instances in minutes for a given month per Airflow environment. Adjust the months to get compute minutes for the most recent 3 months. Read access to the Airflow metadata DB is required.
-- add the results from both queries:

-- Query 1
SELECT 
    SUM(EXTRACT(EPOCH FROM (end_date - start_date)) / 60) AS total_duration_minutes
FROM 
    task_instance
WHERE 
    start_date >= DATE '2024-03-01'
    AND end_date < DATE '2024-04-01'
    AND end_date IS NOT NULL
    AND start_date IS NOT NULL;

-- Query 2
SELECT 
    SUM(EXTRACT(EPOCH FROM (end_date - start_date)) / 60) AS total_duration_minutes
FROM 
    task_instance_history
WHERE 
    start_date >= DATE '2024-03-01'
    AND end_date < DATE '2024-04-01'
    AND end_date IS NOT NULL
    AND start_date IS NOT NULL;
