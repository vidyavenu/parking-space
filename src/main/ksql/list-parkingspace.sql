CREATE STREAM parking_space_availability (st_marker_id VARCHAR, bay_id VARCHAR, lon Double, lat Double, status VARCHAR, magic_id INT) WITH (KAFKA_TOPIC='parking-data', VALUE_FORMAT='json');

CREATE STREAM customer_details (customer_id VARCHAR, lon DOUBLE, lat DOUBLE, magic_id INT) WITH (KAFKA_TOPIC='customer-location', VALUE_FORMAT='json');

CREATE STREAM NEARBY_PARKING_SPACE  AS
SELECT GEO_DISTANCE(customer.LAT, customer.LON, parking.LAT, parking.LON, 'KM') AS DISTANCE, customer.LAT, customer.LON, customer.customer_id, parking.LAT, parking.LON FROM customer_details customer
LEFT JOIN parking_space_availability parking
WITHIN (0 SECONDS, 1 DAYS)
ON customer.magic_id = parking.magic_id
WHERE GEO_DISTANCE(customer.LAT, customer.LON, parking.LAT, parking.LON, 'KM') < 1
EMIT CHANGES;

CREATE STREAM customer_parking_list AS SELECT * FROM NEARBY_PARKING_SPACE PARTITION BY customer_id ;