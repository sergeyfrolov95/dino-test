CREATE TABLE raw_data (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    timeframe_start datetime,
    api_name varchar(100),
    http_method varchar(30),
    count_http_code_5xx int,
    is_anomaly tinyint(1)
    );