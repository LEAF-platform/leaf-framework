from datetime import datetime, timedelta

TEMPLATE: str = """
EQUIPMENT_INSTANCES:
  - equipment:
      adapter: table_simulator
      data:
        instance_id: indpensim/X
        institute: unlock
      requirements:
        write_file: tmpX.csv
        start_date: XXXX-XX-XX XX:XX:XX
        time_column: 'Time (h)' # Timestamp
      simulation:
        filename: /Users/koeho006/git/leaf/leaf/cloud/test_data/indpensim/IndPenSim_V3_Batch_XX.csv.gz
        interval: 0.1

OUTPUTS:
  - plugin: MQTT
    broker: localhost
    port: 1883
    """

# Base start date
base_date = datetime.strptime("2024-08-01 09:00:00", "%Y-%m-%d %H:%M:%S")

for i in range(1, 100):
    config = TEMPLATE
    # Incrementing the date by days and adding an hour
    DATE = base_date + timedelta(days=i) + timedelta(hours=1)
    
    # Replacing placeholders in the template
    config = config.replace("XXXX-XX-XX XX:XX:XX", DATE.strftime("%Y-%m-%d %H:%M:%S"))
    config = config.replace("XX.csv.gz", f"{i}.csv.gz")
    config = config.replace("X", str(i))
    
    # Writing the configuration to a file
    with open(f"config_{i}.yaml", "w") as f:
        f.write(config)
    
