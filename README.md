# Optimizing Ride‑Sharing Operations through Big Data Analysis using Hadoop Map‑Reduce

## 1. Detailed Problem Statement & Analysis Tasks

### Problem Statement
The ride‑sharing industry is rapidly evolving and faces constant pressure to optimize operations and enhance customer satisfaction. In this project, you will analyze a large ride‑sharing dataset to extract actionable insights that can support operational improvements, strategic decision‑making, and targeted marketing initiatives.

**Objective:**  
Using the “Ride‑Sharing Platform Data” dataset, your Map‑Reduce solution will:
- Forecast temporal demand patterns.
- Identify spatial hotspots for ride pickups.
- Analyze fare trends and revenue.
- Evaluate driver performance.
- Segment customers based on ride frequency and fare metrics.

### Analysis Tasks
1. **Temporal Demand Analysis:**  
   - **Input:** Ride records with timestamps.  
   - **Task:** Extract the hour from each timestamp and count the number of rides per hour.  
   - **Expected Output:** A time series (hour vs. ride count) to identify peak demand periods.

2. **Spatial Hotspot Analysis:**  
   - **Input:** Pickup latitude and longitude.  
   - **Task:** Round the coordinates to two decimal places (forming grid cells) and count rides per grid cell.  
   - **Expected Output:** A table (or heatmap) listing grid cells with the corresponding ride counts, highlighting high‑demand areas.

3. **Fare and Revenue Analysis:**  
   - **Input:** Timestamps and fare values.  
   - **Task:** Group rides by date, sum the fare amounts, count rides per day, and compute the average fare.  
   - **Expected Output:** For each day, report total fare, ride count, and average fare.

4. **Driver Performance Evaluation:**  
   - **Input:** Driver IDs and driver ratings.  
   - **Task:** Aggregate ratings by driver to compute each driver’s average rating.  
   - **Expected Output:** A list of drivers with their average ratings for performance evaluation.

5. **Customer Segmentation Analysis:**  
   - **Input:** Customer IDs and fare per ride.  
   - **Task:** Aggregate total rides and total fare per customer; then compute the average fare per ride.  
   - **Expected Output:** Segmented customer profiles detailing total rides, total fare, and average fare to support targeted marketing.

---

## 2. Dataset & Source Information

**Dataset:** Ride‑Sharing Platform Data  
**Source:** Kaggle  
**Link:** [https://www.kaggle.com/datasets/adnananam/ride-sharing-platform-data]

**Dataset Details:**  
- **Size:** Contains over 10,000 records.
- **Fields/Attributes:**  
  1. **ride_id**  
  2. **timestamp** (format: YYYY‑MM‑DD HH:MM:SS)  
  3. **pickup_lat**  
  4. **pickup_lon**  
  5. **dropoff_lat**  
  6. **dropoff_lon**  
  7. **fare**  
  8. **driver_id**  
  9. **driver_rating**  
  10. **customer_id**

*Note:* Adjust field indices if your CSV structure differs.

---

## 3. Map‑Reduce Diagrams for Each Analysis Task

### Diagram 1: Temporal Demand Analysis
```
[Input CSV]
      │
      ▼   (Mapper: Extract hour from timestamp → emit (hour, 1))
[Shuffle/Sort: Group records by hour]
      │
      ▼   (Reducer: Sum counts per hour)
[Output] → (hour, total_rides)
```

### Diagram 2: Spatial Hotspot Analysis
```
[Input CSV]
      │
      ▼   (Mapper: Round pickup_lat & pickup_lon to 2 decimals → emit (grid_cell, 1))
[Shuffle/Sort: Group records by grid_cell]
      │
      ▼   (Reducer: Sum counts per grid cell)
[Output] → (grid_cell, ride_count)
```

### Diagram 3: Fare and Revenue Analysis
```
[Input CSV]
      │
      ▼   (Mapper: Extract date and fare → emit (date, "fare,1"))
[Shuffle/Sort: Group records by date]
      │
      ▼   (Reducer: Sum fares and counts; compute average fare)
[Output] → (date, total_fare, ride_count, avg_fare)
```

### Diagram 4: Driver Performance Evaluation
```
[Input CSV]
      │
      ▼   (Mapper: Extract driver_id & rating → emit (driver_id, "rating,1"))
[Shuffle/Sort: Group records by driver_id]
      │
      ▼   (Reducer: Sum ratings and counts; compute average rating)
[Output] → (driver_id, avg_rating)
```

### Diagram 5: Customer Segmentation Analysis
```
[Input CSV]
      │
      ▼   (Mapper: Extract customer_id & fare → emit (customer_id, "fare,1"))
[Shuffle/Sort: Group records by customer_id]
      │
      ▼   (Reducer: Aggregate total fare and count rides; compute average fare)
[Output] → (customer_id, total_rides, total_fare, avg_fare)
```

---

## 4. Pseudo Code & Functional Code for All Map‑Reduce Tasks

### Task 1: Temporal Demand Analysis

**Pseudo Code:**
```
For each record:
    Parse timestamp to extract hour.
    Emit (hour, 1)

For each hour group:
    Sum all counts.
    Emit (hour, total_rides)
```

**Mapper (temporal_mapper.py):**
```python
#!/usr/bin/env python3
import sys
from datetime import datetime

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 2:
        continue
    timestamp = fields[1]
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        hour = dt.hour
        print(f"{hour}\t1")
    except Exception:
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

---

### Task 2: Spatial Hotspot Analysis

**Pseudo Code:**
```
For each record:
    Parse pickup_lat and pickup_lon.
    Round both values to 2 decimals to form grid_cell.
    Emit (grid_cell, 1)

For each grid_cell group:
    Sum all counts.
    Emit (grid_cell, ride_count)
```

**Mapper (spatial_mapper.py):**
```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 4:
        continue
    try:
        pickup_lat = float(fields[2])
        pickup_lon = float(fields[3])
        grid_lat = round(pickup_lat, 2)
        grid_lon = round(pickup_lon, 2)
        print(f"{grid_lat},{grid_lon}\t1")
    except Exception:
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

---

### Task 3: Fare and Revenue Analysis

**Pseudo Code:**
```
For each record:
    Parse timestamp and fare.
    Extract date from timestamp.
    Emit (date, "fare,1")

For each date group:
    Sum all fares and counts.
    Compute average fare.
    Emit (date, total_fare, ride_count, avg_fare)
```

**Mapper (fare_mapper.py):**
```python
#!/usr/bin/env python3
import sys
from datetime import datetime

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 7:
        continue
    timestamp = fields[1]
    fare = fields[6]
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        day = dt.strftime('%Y-%m-%d')
        fare_value = float(fare)
        print(f"{day}\t{fare_value},1")
    except Exception:
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

---

### Task 4: Driver Performance Evaluation

**Pseudo Code:**
```
For each record:
    Parse driver_id and driver_rating.
    Emit (driver_id, "rating,1")

For each driver_id group:
    Sum ratings and counts.
    Compute average rating.
    Emit (driver_id, avg_rating)
```

**Mapper (driver_mapper.py):**
```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 9:
        continue
    driver_id = fields[7]
    driver_rating = fields[8]
    try:
        rating = float(driver_rating)
        print(f"{driver_id}\t{rating},1")
    except Exception:
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

---

### Task 5: Customer Segmentation Analysis

**Pseudo Code:**
```
For each record:
    Parse customer_id and fare.
    Emit (customer_id, "fare,1")

For each customer_id group:
    Sum fares and ride counts.
    Compute average fare.
    Emit (customer_id, total_rides, total_fare, avg_fare)
```

**Mapper (customer_mapper.py):**
```python
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) < 10:
        continue
    customer_id = fields[9]
    fare = fields[6]
    try:
        fare_value = float(fare)
        print(f"{customer_id}\t{fare_value},1")
    except Exception:
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

---

## 5. Execution Statistics

For each Map‑Reduce job, record the following metrics (to be obtained from your Hadoop job tracker or logs):

- **Number of Map Tasks:** e.g., 50 mappers  
- **Number of Reduce Tasks:** e.g., 10 reducers  
- **Memory Consumption per Task:** e.g., 1GB per mapper/reducer (as observed)  
- **Bytes Transferred:** Total amount of data shuffled between mappers and reducers

Summarize these details in a table in your final documentation.

---

## Final Submission Deliverables

Your final PDF (“Group‑[number].pdf”) must include:

1. **Detailed Problem Statement & Analysis Tasks:**  
   - A clear explanation of the problem and objectives.
   - Descriptions of each of the five analysis tasks, including input and expected output.

2. **Dataset & Source Information:**  
   - A detailed description of the “Ride‑Sharing Platform Data” dataset (including the Kaggle link provided above) and any preprocessing performed.

3. **Map‑Reduce Diagrams:**  
   - Diagrams for each analysis task showing the data flow from input → mapper → shuffle/sort → reducer → output.

4. **Pseudo Code & Functional Code:**  
   - Full pseudo code and complete Python scripts (mapper and reducer) for all five Map‑Reduce tasks.

5. **Execution Statistics:**  
   - A summary table detailing Hadoop job metrics for each task (number of tasks, memory usage, bytes transferred).

---

This document provides a comprehensive, end‑to‑end solution for the assignment using a dataset with over 10,000 records. Adjust file paths, field indices, and configuration parameters as necessary for your Hadoop environment and dataset specifics before submission.
