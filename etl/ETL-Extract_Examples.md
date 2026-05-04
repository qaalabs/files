# ETL (Extract, Transform, Load)

## Streaming, batching, and on-demand services

#### Setup


```python
import pandas as pd
import time
import random
from datetime import datetime
import json
import requests
```


```python
# Sample streaming data generator
def generate_customer_update():
    """Simulate a real-time customer update"""
    return {
        'timestamp': datetime.now().isoformat(),
        'customer_id': random.randint(1001, 1099),
        'event_type': random.choice(['purchase', 'login', 'support_ticket', 'address_change']),
        'value': random.randint(10, 500)
    }
```

### Batch


```python
# Batch: Process 100 updates at once
updates = [generate_customer_update() for _ in range(100)]
print(f"Batch processing: {len(updates)} updates processed at {datetime.now()}")
```


```python
# Batch: Show the first 5 records returned
for i in range(5):
    print(updates[i])
```

### Streaming


```python
# Streaming: Process updates as they "arrive"
print("STREAMING SIMULATION - Press 'Interrupt' to stop")
print("=" * 50)

processed_count = 0
try:
    while processed_count < 20:  # Limit for demo purposes
        # Simulate new data arriving
        update = generate_customer_update()
        
        # Process immediately
        print(f"{update['timestamp'][:19]} | Customer {update['customer_id']} | {update['event_type']} | £{update['value']}")
        
        # Your processing logic here
        if update['event_type'] == 'support_ticket':
            print("   ALERT: Support ticket requires immediate attention!")
        
        processed_count += 1
        time.sleep(2)  # Wait 2 seconds for next "event"
        
except KeyboardInterrupt:
    print(f"\nStreaming Interrupted!")

print(f"\nStreaming completed. Processed {processed_count} events.")
```

### On-demand


```python
# On-demand: Process when requested
def lookup_customer_activity(customer_id):
    """Simulate on-demand customer lookup"""
    # This would typically query a database
    activity = generate_customer_update()
    activity['customer_id'] = customer_id
    return activity
```


```python
# Simulate customer service agent looking up customer
result = lookup_customer_activity(1005)
print(f"Customer {result['customer_id']} recent activity: {result}")
```

### API


```python
# Get the data from an api
postcode = "SW1A1AA"
url = f"https://api.postcodes.io/postcodes/{postcode}"
postcode_data = requests.get(url).json()
```


```python
# print the result
print(f"Status: {postcode_data['status']}")
print(f"Country: {postcode_data['result']['country']}")
print(f"Region: {postcode_data['result']['region']}")
print(f"Constituency: {postcode_data['result']['parliamentary_constituency']}")
#print(f"Status: {postcode_data['result']}")
```

### CSV


```python
# Call the function to generate a batch of updates
updates = [generate_customer_update() for _ in range(10)]
df = pd.DataFrame(updates)
df.to_csv("customers.csv", index=False)
```


```python
# Load CSV into a DataFrame
df_customer = pd.read_csv("customers.csv")
print(df_customer.head())
```


```python

```
