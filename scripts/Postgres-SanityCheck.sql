-- Postgres SQL queries for Sanity Check: (SANITY_CHECK flag should be set as true to enable this functionality)

CREATE TABLE mappings(
 a_id VARCHAR (50) PRIMARY KEY,
 c_id integer
);

COPY mappings FROM '/home/vinu/mapping.tsv';

CREATE TABLE result(
 w_id integer,
 c_id VARCHAR(35) NOT NULL,
 count bigint,
PRIMARY KEY (w_id, c_id)
);

CREATE TABLE data(
 event_time VARCHAR NOT NULL,
 w_id integer,
 rank integer,
 iteration integer,
 event_type VARCHAR NOT NULL,
 a_id VARCHAR  NOT NULL,
PRIMARY KEY (event_time, rank, iteration)
);


delete from data;

delete from result;

delete from mappings;

COPY data FROM '/home/vinu/AIR1.1/Release/Data/data0.tsv';

COPY result FROM '/home/vinu/AIR1.1/Release/Data/results0.tsv';

COPY mappings FROM '/home/vinu/AIR1.1/Release/Data/mappings.tsv';

select a.w_id, sum (r.count) as obtained, sum (a.actual_count) as actual
from result as r, 
(select d.w_id as w_id, m.c_id as c_id, count(d.event_type) as actual_count
from data as d, mappings as m
where d.event_type = 'click' and d.a_id = m.a_id
group by d.w_id, m.c_id
order by d.w_id) as a
where a.w_id = r.w_id and a.c_id = r.c_id
group by a.w_id
order by a.w_id

-- ALTERNATIVELY

select a.w_id, a.c_id, a.actual_count as actual_count, r.count as result_count, case when a.actual_count=r.count then 'true' else '****false****' end as correctness
from result as r, 
(select d.w_id as w_id, m.c_id as c_id, count(d.event_type) as actual_count
from data as d, mappings as m
where d.event_type = 'click' and d.a_id = m.a_id
group by d.w_id, m.c_id
order by d.w_id) as a
where a.w_id = r.w_id and a.c_id = r.c_id
order by a.w_id


-- Postgres SQL queries for throughput calculation:

CREATE TABLE throughputs(
throughput bigint, 
rank integer,
time bigint
);

delete from throughputs;

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log0.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log1.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log2.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log3.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log4.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log5.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log6.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log7.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log8.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log9.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log10.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log11.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log12.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log13.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log14.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log15.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log16.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log17.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log18.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log19.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log20.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log21.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log22.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log23.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log24.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log25.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log26.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log27.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log28.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log29.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log30.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log31.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log32.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log33.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log34.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log35.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log36.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log37.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log38.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log39.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log40.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log41.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log42.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log43.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log44.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log45.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log46.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log47.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log48.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log49.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log50.tsv';

COPY throughputs FROM '/home/vinu/AIR1.1/Release/Data/tp_log51.tsv';

select avg(A.tp) from 
(select time, sum(throughput) as tp
from throughputs
group by time) as A

select time, sum(throughput)
from throughputs
group by time
order by time






