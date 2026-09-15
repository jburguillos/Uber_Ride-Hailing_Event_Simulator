
# Ride-Hailing Event Simulator

This Python tool generates a sequence of synthetic ride and passenger request events and streams them into Azure Event Hubs (using the Kafka protocol). The goal is to create realistic data—complete with surges, cancellations, and variable trip durations—that you can feed into Spark Streaming jobs, Streamlit dashboards, or any analytics pipeline.

The simulator is entirely driven by `config.json`, which means you can change the total number of rides, driver availability, pricing rules, and pacing without touching the code. When the script runs, it follows these main steps:

1. **Load Configuration**  
   The script opens `config.json` and reads:
   - `num_requests`: how many ride events to attempt in total.  
   - `active_drivers`: number of drivers available (higher values mean more assigned rides).  
   - `event_interval_sec`: seconds to sleep between sending each event (throttling).  
   - `simulation_days`: number of days over which to spread the events.  
   - `pricing_model`: a nested object containing  
     - `base_price`: flat fare component.  
     - `price_per_km`: distance-based component.  
     - `tip_rate`: probability and percentage of tipping.  
     - `surge_zones`: mapping of zone names (e.g., Airport, Downtown, Nightlife) to multipliers.

2. **Initialize Kafka Producer**  
   At the top of the script you configure a `confluent_kafka.Producer` by specifying:
   - `bootstrap.servers`: your Event Hubs namespace and port.  
   - SASL settings (`security.protocol`, `sasl.mechanism`, `sasl.username`, `sasl.password`).  
   This producer will publish binary Avro messages to two topics:  
   - `group1_ride_status`  
   - `group1_passenger_requests`

3. **Parse Avro Schemas**  
   Using `fastavro.parse_schema`, two in-memory schemas are created:
   - **RideStatus**: captures the full lifecycle of a trip (IDs, request/pickup/dropoff timestamps, distance, price, tip, vehicle type, status, cancellation reason).  
   - **PassengerRequest**: logs the initial request with timestamp, locations, distance, status, and payment type.

4. **Define Helper Functions**  
   - `get_demand_multiplier(hour, zone, vehicle_type)`: returns a probability (0–1) that a new ride request occurs, based on time of day, zone characteristics, and vehicle type, plus a small random noise.  
   - `get_duration_multiplier(hour, zone)`: inflates trip duration during rush hours or late-night in busy areas, again adding randomness.

5. **Generate Individual Events**  
   The `generate_ride_event(sim_time)` function:
   - Uses the demand multiplier to decide whether to skip this slot or produce a ride.  
   - Assigns unique UUIDs to ride, passenger, and (sometimes) driver.  
   - Chooses random pickup/dropoff zones and computes timestamps:  
     - `request_time` from `sim_time`.  
     - A random `pickup_delay` (30–300s).  
     - Base duration (300–900s) multiplied by the duration multiplier.  
   - Picks a random distance (1–20 km, with rare large outliers).  
   - Calculates fare: `base_price + distance * price_per_km * surge_multiplier` and applies a tip with probability `tip_rate`.  
   - Applies cancellation logic: in some zones (Nightlife, Suburbs), rides may cancel, which zeroes out duration, price, and tip.  
   - Builds two Python dictionaries: one for the ride status and one for the passenger request.

6. **Main Simulation Loop**  
   - Compute `total_minutes = simulation_days * 24 * 60`.  
   - Determine `events_per_minute = num_requests / total_minutes`.  
   - For each minute slot:  
     1. Call `generate_ride_event` up to `events_per_minute` times.  
     2. If a ride and request record are returned:  
        - Serialize each with `fastavro.schemaless_writer` into an in-memory buffer.  
        - Send the buffer bytes via `producer.produce()` to the appropriate topic and flush immediately.  
        - Print a green log message.  
     3. Otherwise, print a red log message for skipped slots.  
     4. Sleep for `event_interval_sec` seconds before the next event.  
   - After all minutes, print a summary of how many events were actually generated versus attempted.

7. **Running the Script**  
   - Ensure you have Python 3.7+ and the dependencies installed: pip install fastavro confluent-kafka
   - Place Milestone 1.4 - Final Final.py and config.json in the same folder, update your Event Hubs credentials in the script

8. **Consuming the Data**
   - Downstream, you can subscribe to the two Kafka topics from any consumer:
     - Spark Streaming (or Structured Streaming) jobs for live analytics.
     - Streamlit or other dashboards for real-time visualization.
     - Batch processes by adapting the script to write JSON files instead of sending to Kafka.

This setup gives you a full end-to-end pipeline to experiment with real-time event streams without involving actual user data.
