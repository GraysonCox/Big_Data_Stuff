-- Load data.
initial_data = LOAD '/cpre419/network_trace' USING PigStorage(' ');

-- Drop unnecessary fields.
events = FOREACH initial_data GENERATE
	$2 AS source_ip: chararray,
	$4 AS destination_ip: chararray,
	$5 AS protocol: chararray;

-- Remove everything that isn't TCP.
tcp_events = FILTER events BY protocol == 'tcp' OR protocol == 'TCP';

-- Remove unnecessary digits from IPs.
tcp_events_ip_trimmed = FOREACH tcp_events {
	GENERATE SUBSTRING(source_ip, 0, LAST_INDEX_OF(source_ip, '.')) AS source_ip_trimmed,
		SUBSTRING(destination_ip, 0, LAST_INDEX_OF(destination_ip, '.')) AS destination_ip_trimmed;
}

-- Group by source IP.
destinations_for_source = GROUP tcp_events_ip_trimmed BY source_ip_trimmed;

-- Count all unique destination IPs for each source IP.
num_unique_destinations_for_source = FOREACH destinations_for_source {
	unique_destinations = DISTINCT tcp_events_ip_trimmed.destination_ip_trimmed;
	GENERATE group, COUNT(unique_destinations) as num_unique_destinations;
}

-- Sort by number of unique destination IP's.
sorted_num_unique_destinations_for_source = ORDER num_unique_destinations_for_source BY num_unique_destinations DESC;

-- Get top ten.
top_ten_sources = LIMIT sorted_num_unique_destinations_for_source 10;

-- Store results.
STORE top_ten_sources INTO '/lab4/exp2/output/';
