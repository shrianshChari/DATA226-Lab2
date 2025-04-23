# DATA 226 Lab 2

### How to set up
1. Clone the repository
2. Start the Docker container
```bash
docker compose up
```
3. Go to Admin -> Connections and set up your Snowflake account. You should have the following fields filled out:
    - Login (Username)
    - Password
    - Account
    - Warehouse
    - Database
    - Role
4. Go to Admin -> Variables and fill out the following fields:
    - `snowflake_database` - Same database as above
    - `snowflake_schema` - Schema to put the raw data for ETL (basically anything but "analytics"; I use "raw")
    - `snowflake_table` - Table in the aforementioned schema to put the raw data; I use "stock_prices"
    - `snowflake_model` - The name of the forecasting model to be created; I use "m"
    - `snowflake_forecast_table` - Name of the table to put the results of the forecast; I use "forecasted"
    - `snowflake_final_table` - Name of the table that is produced by unioning the raw and forecasted table; I use "final"
