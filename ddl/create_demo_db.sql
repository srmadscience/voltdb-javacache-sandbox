



CREATE PROCEDURE getSchemeMember 
PARTITION ON TABLE KV COLUMN K PARAMETER 1
AS
select k
     , field(varbinary_to_varchar(v),'loyaltySchemeNumber') loyaltySchemeNumber
     , field(varbinary_to_varchar(v),'loyaltySchemeName') loyaltySchemeName
     , field(varbinary_to_varchar(v),'loyaltySchemeTier')  loyaltySchemeTier
     , field(varbinary_to_varchar(v),'loyaltySchemePoints') loyaltySchemePoints
from kv
where c = ?
and   k = ?;

CREATE VIEW kv_table_stats AS
SELECT c, count(*) how_many from kv
GROUP BY c;

CREATE VIEW kv_deltas_stats AS
SELECT c,event_type , count(*) how_many 
FROM kv_deltas
GROUP BY c, event_type;

CREATE PROCEDURE kv__promBL AS
BEGIN
select 'category_'||c||'_parameter_'||param_name statname,  'parameter name' stathelp  ,param_value statvalue from kv_parameters order by param_name;
select 'category_'||c statname,  'kv classes' stathelp  ,how_many statvalue from kv_table_stats order by c;
select 'delta_'||c ||'_'||event_type statname,  'kv deltas' stathelp  ,how_many statvalue from kv_deltas_stats order by c, event_type;
END;

