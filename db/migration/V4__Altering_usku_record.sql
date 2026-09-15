alter table usku_record
drop primary key,
drop index usku_id,
add primary key(usku_id),
drop column indx;