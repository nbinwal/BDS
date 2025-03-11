# BDS
Below is a complete, end‐to‐end solution for the ride‑sharing analysis assignment. In this example, we use a public ride‑sharing dataset (with over 10,000 records) that contains the following CSV fields:

```
ride_id,timestamp,pickup_lat,pickup_lon,dropoff_lat,dropoff_lon,fare,driver_id,driver_rating,customer_id
```

We then implement five different analysis tasks using Hadoop Map‑Reduce. For each task, two Python scripts (a mapper and a reducer) are provided. You can run these with Hadoop Streaming on your Hadoop cluster (e.g., on the BITS Nuvepro Lab Platform). Adjust the field indices and formats as needed for your actual dataset.

---

## 1. Problem Statement & Analysis Tasks

**Problem Statement:**  
“Optimizing Ride‑Sharing Operations through Big Data Analysis”  
This study uses a large ride‑sharing dataset to:
- Forecast temporal demand,
- Identify geographic hotspots,
- Analyze fare trends and revenue,
- Evaluate driver performance,
- Aggregate customer ride metrics for segmentation.

Each task is implemented as a separate Map‑Reduce job.

---

## 2. Analysis Tasks and Map‑Reduce Code

### Task 1: Temporal Demand Analysis

**Objective:**  
Count the number of rides per hour.

**Mapper (temporal_mapper.py):**
```python
#!/usr/bin/env python3
import sys
from datetime import datetime

for line in sys.stdin:
    # Remove header if necessary; here we assume each line is a valid record.
    fields = line.strip().split(',')
    if len(fields) < 2:
        continue
    timestamp = fields[1]  # Assuming timestamp is the 2nd column
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        hour = dt.hour  # Extract hour (0-23)
        print(f"{hour}\t1")
    except Exception as e:
        continue
```

**Reducer (temporal_reducer.py):**
```python
#!/usr/bin/env python3
import sys

current_hour = None
current_count = 0

for line in sys.stdin:
    hour, count = line.strip().split('\t')
    count = int(count)
    if current_hour == hour:
        current_count += count
    else:
        if current_hour is not None:
            print(f"{current_hour}\t{current_count}")
        current_hour = hour
        current_count = count

if current_hour is not None:
    print(f"{current_hour}\t{current_count}")
```

*Diagram:*

```
[Input CSV] → [Mapper: Extract Hour → emit (hour,1)]
         ↓
   [Shuffle/Sort: Group by hour]
         ↓
   [Reducer: Sum counts per hour] → (hour, total_rides)
```

---

### Task 2: Spatial Hotspot Analysis

**Objective:**  
Count rides per pickup grid cell by rounding latitude and longitude to two decimals.

**Mapper (spatial_mapper.py):**
```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 4:
        continue
    try:
        pickup_lat = float(fields[2])  # 3rd field
        pickup_lon = float(fields[3])  # 4th field
        # Round to 2 decimal places to create grid cells
        grid_lat = round(pickup_lat, 2)
        grid_lon = round(pickup_lon, 2)
        print(f"{grid_lat},{grid_lon}\t1")
    except:
        continue
```

**Reducer (spatial_reducer.py):**
```python
#!/usr/bin/env python3
import sys

current_cell = None
cell_count = 0

for line in sys.stdin:
    cell, count = line.strip().split('\t')
    count = int(count)
    if current_cell == cell:
        cell_count += count
    else:
        if current_cell is not None:
            print(f"{current_cell}\t{cell_count}")
        current_cell = cell
        cell_count = count

if current_cell is not None:
    print(f"{current_cell}\t{cell_count}")
```

*Diagram:*

```
[Input CSV] → [Mapper: Round pickup_lat & pickup_lon → emit (grid_cell,1)]
         ↓
 [Shuffle/Sort: Group by grid_cell]
         ↓
 [Reducer: Sum counts per grid_cell] → (grid_cell, ride_count)
```

---

### Task 3: Fare and Revenue Analysis

**Objective:**  
Aggregate total and average fare per day.

**Mapper (fare_mapper.py):**
```python
#!/usr/bin/env python3
import sys
from datetime import datetime

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 7:
        continue
    timestamp = fields[1]  # 2nd column
    fare = fields[6]       # 7th column
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        day = dt.strftime('%Y-%m-%d')  # Extract date portion
        fare_value = float(fare)
        # Emit fare and a count (fare,1)
        print(f"{day}\t{fare_value},1")
    except:
        continue
```

**Reducer (fare_reducer.py):**
```python
#!/usr/bin/env python3
import sys

current_day = None
total_fare = 0.0
total_count = 0

for line in sys.stdin:
    day, value = line.strip().split('\t')
    fare_str, count_str = value.split(',')
    fare_val = float(fare_str)
    count_val = int(count_str)
    if current_day == day:
        total_fare += fare_val
        total_count += count_val
    else:
        if current_day is not None and total_count > 0:
            avg_fare = total_fare / total_count
            print(f"{current_day}\tTotal_Fare: {total_fare:.2f}\tCount: {total_count}\tAvg_Fare: {avg_fare:.2f}")
        current_day = day
        total_fare = fare_val
        total_count = count_val

if current_day is not None and total_count > 0:
    avg_fare = total_fare / total_count
    print(f"{current_day}\tTotal_Fare: {total_fare:.2f}\tCount: {total_count}\tAvg_Fare: {avg_fare:.2f}")
```

*Diagram:*

```
[Input CSV] → [Mapper: Extract date, fare → emit (date, "fare,1")]
         ↓
    [Shuffle/Sort: Group by date]
         ↓
    [Reducer: Sum fares and counts → compute average] 
         → (date, total_fare, ride_count, average_fare)
```

---

### Task 4: Driver Performance Evaluation

**Objective:**  
Compute the average driver rating for each driver.

**Mapper (driver_mapper.py):**
```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 9:
        continue
    driver_id = fields[7]       # 8th field: driver_id
    driver_rating = fields[8]   # 9th field: driver_rating
    try:
        rating = float(driver_rating)
        print(f"{driver_id}\t{rating},1")
    except:
        continue
```

**Reducer (driver_reducer.py):**
```python
#!/usr/bin/env python3
import sys

current_driver = None
total_rating = 0.0
total_count = 0

for line in sys.stdin:
    driver, value = line.strip().split('\t')
    rating_str, count_str = value.split(',')
    rating_val = float(rating_str)
    count_val = int(count_str)
    if current_driver == driver:
        total_rating += rating_val
        total_count += count_val
    else:
        if current_driver is not None and total_count > 0:
            avg_rating = total_rating / total_count
            print(f"{current_driver}\tAvg_Rating: {avg_rating:.2f}")
        current_driver = driver
        total_rating = rating_val
        total_count = count_val

if current_driver is not None and total_count > 0:
    avg_rating = total_rating / total_count
    print(f"{current_driver}\tAvg_Rating: {avg_rating:.2f}")
```

*Diagram:*

```
[Input CSV] → [Mapper: Extract driver_id & rating → emit (driver_id, "rating,1")]
         ↓
   [Shuffle/Sort: Group by driver_id]
         ↓
   [Reducer: Sum ratings & counts → compute average rating] 
         → (driver_id, avg_rating)
```

---

### Task 5: Customer Segmentation Analysis

**Objective:**  
Aggregate metrics per customer: total rides, total fare, and average fare.

**Mapper (customer_mapper.py):**
```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 10:
        continue
    customer_id = fields[9]     # 10th field: customer_id
    fare = fields[6]            # 7th field: fare
    try:
        fare_value = float(fare)
        print(f"{customer_id}\t{fare_value},1")
    except:
        continue
```

**Reducer (customer_reducer.py):**
```python
#!/usr/bin/env python3
import sys

current_customer = None
total_fare = 0.0
ride_count = 0

for line in sys.stdin:
    customer, value = line.strip().split('\t')
    fare_str, count_str = value.split(',')
    fare_val = float(fare_str)
    count_val = int(count_str)
    if current_customer == customer:
        total_fare += fare_val
        ride_count += count_val
    else:
        if current_customer is not None and ride_count > 0:
            avg_fare = total_fare / ride_count
            print(f"{current_customer}\tRides: {ride_count}\tTotal_Fare: {total_fare:.2f}\tAvg_Fare: {avg_fare:.2f}")
        current_customer = customer
        total_fare = fare_val
        ride_count = count_val

if current_customer is not None and ride_count > 0:
    avg_fare = total_fare / ride_count
    print(f"{current_customer}\tRides: {ride_count}\tTotal_Fare: {total_fare:.2f}\tAvg_Fare: {avg_fare:.2f}")
```

*Diagram:*

```
[Input CSV] → [Mapper: Extract customer_id & fare → emit (customer_id, "fare,1")]
         ↓
  [Shuffle/Sort: Group by customer_id]
         ↓
  [Reducer: Sum total fare & count rides, compute average fare]
         → (customer_id, total_rides, total_fare, avg_fare)
```

---

## 3. Execution Statistics

When running each Map‑Reduce job on your Hadoop cluster, capture the following details (from your job tracker/logs):
- **Number of Map Tasks:** e.g., 50 tasks
- **Number of Reduce Tasks:** e.g., 10 tasks
- **Memory Consumption per Task:** Reported in the Hadoop logs (e.g., 1GB per task)
- **Bytes Transferred:** Data shuffled between mappers and reducers

Include these statistics in your final documentation.

---

## 4. Final Documentation & Submission

Compile your deliverable as a single PDF document that includes:

1. **Detailed Problem Statement & Analysis Tasks:** As described above.
2. **Dataset & Source Information:** Include a description and source link for your ride‑sharing dataset.
3. **Map‑Reduce Diagrams:** Provide diagrams for each analysis task (as shown above).
4. **Pseudo Code & Functional Code:** Include the full Python code (for mappers and reducers) for all five tasks.
5. **Execution Statistics:** Tabulate and summarize the statistics captured during job execution.

Name your final PDF document as per the guidelines (e.g., “Group-[number].pdf”) and submit by the deadline.

---

This complete solution not only outlines a clear action plan for the assignment but also provides full working code for all five analysis tasks using Hadoop Map‑Reduce. Adjust the code and parameters as necessary for your dataset and cluster configuration.
