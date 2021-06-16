


load classes ../lib/gson-2.2.2.jar;


CREATE PROCEDURE getSchemeMember AS
select k
     , field(varbinary_to_varchar(v),'loyaltySchemeNumber') loyaltySchemeNumber
     , field(varbinary_to_varchar(v),'loyaltySchemeName') loyaltySchemeName
     , field(varbinary_to_varchar(v),'loyaltySchemeTier')  loyaltySchemeTier
     ,  field(varbinary_to_varchar(v),'loyaltySchemePoints') loyaltySchemePoints
from kv
where c = ?
and   k = ?;
