-- Load ip_trace, trim the IPs, and drop unnecessary fields.
ip_trace = LOAD '/cpre419/ip_trace' USING PigStorage(' ');
ip_trace = FOREACH ip_trace GENERATE
	$0 AS time: chararray,
	$1 AS connection_id: int,
	$2 AS source_ip: chararray,
	$4 AS destination_ip: chararray;

-- Load raw_block.
raw_block = LOAD '/cpre419/raw_block' USING PigStorage(' ') AS (connection_id: int, action: chararray);

-- Inner join the bags on connection_id.
actions = JOIN ip_trace BY connection_id, raw_block BY connection_id;

-- Get all blocking records.
actions = FILTER actions BY action == 'Blocked';

-- Sort by timestamp.
actions = ORDER actions BY time ASC;

-- Store firewall log file.
STORE actions INTO '/lab4/exp3/firewall/';

-- Group by source IP.
source_ip_groups = GROUP actions BY source_ip;

-- Count number of times each source IP has been blocked.
num_times_blocked_for_source_ip = FOREACH source_ip_groups {
	GENERATE group AS source_ip, COUNT(actions.source_ip) AS num_blocks;
}

-- Sort by most blocked.
num_times_blocked_for_source_ip = ORDER num_times_blocked_for_source_ip BY num_blocks DESC;

-- Store results.
STORE num_times_blocked_for_source_ip INTO '/lab4/exp3/output/';
