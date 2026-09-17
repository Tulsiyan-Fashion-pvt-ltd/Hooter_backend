alter table brand_access
drop foreign key brand_access_ibfk_3
add foreign key(user_id) references users(user_id) on delete cascade;


