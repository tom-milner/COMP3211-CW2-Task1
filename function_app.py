import logging
import azure.functions as func
import os
import random
import pyodbc

# pyodbc needs the SQL driver aswell as the SQL connection string for connecting.
sql_driver = "Driver={ODBC Driver 18 for SQL Server};"

# Get Azure Application Settings.
db_name = os.environ["DatabaseName"]
table_name = os.environ["TableName"]
sql_connection_string = sql_driver + os.environ["SqlConnectionString"]

app = func.FunctionApp()


# Azure function to simulate environment sensor readings, set to run every 5 seconds.
@app.function_name(name="generate_sensor_readings")
@app.timer_trigger(
    schedule="*/5 * * * * *",
    arg_name="timer",
    run_on_startup=True,
    use_monitor=False,
)
def generate_sensor_readings(timer: func.TimerRequest) -> None:
    if timer.past_due:
        logging.info("The timer is past due!")

    # Limits:
    # Temperature:          8 - 15
    # Wind Speed:           15 - 25
    # Relative Humidity:    40 - 70
    # CO2:                  500 - 1500

    # Generate readings.
    num_sensors = 20
    readings_per_sensor = 10
    readings = list()
    for i in range(num_sensors):
        for j in range(readings_per_sensor):
            # NOTE: randrange is min inclusive, max exclusive (hence the +1)
            reading = {
                "sensor_id": i,
                "temp": random.randrange(8, 15 + 1),
                "wind_speed": random.randrange(15, 25 + 1),
                "rel_humidity": random.randrange(40, 70 + 1),
                "co2": random.randrange(500, 1500 + 1),
            }
            readings.append(reading)

    logging.info(f"Generated {len(readings)} readings.")

    # Save readings in database.
    conn = pyodbc.connect(sql_connection_string)
    cur = conn.cursor()

    # If the sensor readings table doesn't exist, create it.
    if not cur.tables(table=table_name, tableType="TABLE").fetchone():
        logging.info(f"Creating table {table_name}.")
        create_table_sql = f"CREATE TABLE {table_name}(id int IDENTITY(1,1) PRIMARY KEY, sensor_id int, temp int, wind_speed int, rel_humidity int, co2 int)"
        cur.execute(create_table_sql)
        conn.commit()
        logging.info("Table created.")

    else:
        logging.info(f"Table {table_name} exists.")

    # Insert the readings into the database.
    for r in readings:
        cur.execute(
            f"insert into {table_name}(sensor_id, temp, wind_speed, rel_humidity, co2) values (?,?,?,?,?)",
            r["sensor_id"],
            r["temp"],
            r["wind_speed"],
            r["rel_humidity"],
            r["co2"],
        )
    conn.commit()
    conn.close()
