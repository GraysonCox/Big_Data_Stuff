-- Load data.
initial_data = LOAD 'gaz_tracts_national.txt' USING PigStorage('\t')
	AS (usps:chararray, geoid:long, pop10:int, hu10:int, aland:long, 
	awater:int, aland_sqmi:double, awater_sqmi:double, intptlat:double, intptlong:double);

-- Group by state.
states = GROUP initial_data BY usps;

-- Find total land area for each state.
states_with_land_area = FOREACH states GENERATE FLATTEN(group), SUM(initial_data.aland) AS total_land_area;

-- Sort by land area.
sorted_states = ORDER states_with_land_area BY total_land_area DESC;

-- Get top ten tuples.
top_ten_states = LIMIT sorted_states 10;

-- Store data.
STORE top_ten_states INTO 'exp1_output/';
