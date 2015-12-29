CREATE TABLE rti.kpi_aaa (
id bigint not null, 
subscriber_id varchar(64) not null,
cell_id varchar(30),
tac varchar(8),
time_from timestamp not null,
aggregation_interval int,
input_octets bigint,
output_octets bigint,
input_packets bigint,
output_packets bigint,
count_2g int,
count_3g int,
count_4g int,
time_new timestamp default '1970-01-01 00:00:00',
worker_id smallint default 0,
constraint kpi_aaa_pk primary key ( subscriber_id, cell_id, tac, time_from, aggregation_interval )
)
PARTITION BY COLUMN( subscriber_id ) REDUNDANCY 0
EXPIRE ENTRY WITH TIMETOLIVE 270 ACTION DESTROY;

CREATE UNIQUE INDEX idx_kpi_aaa_pk ON rti.kpi_aaa ( subscriber_id, cell_id, tac, time_from, aggregation_interval );
CREATE INDEX idx_kpi_aaa_subscriber_id ON rti.kpi_aaa ( subscriber_id );
CREATE INDEX idx_kpi_aaa_tac ON rti.kpi_aaa ( tac );
CREATE INDEX idx_kpi_aaa_cell_id ON rti.kpi_aaa ( cell_id );
CREATE INDEX idx_kpi_aaa_time_from ON rti.kpi_aaa ( time_from );
CREATE INDEX idx_kpi_aaa_aggregation_interval ON rti.kpi_aaa ( aggregation_interval );
CREATE INDEX idx_kpi_aaa_worker_id ON rti.kpi_aaa ( worker_id );
