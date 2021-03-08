CREATE TYPE award AS ENUM (
	'Academy'
	'Golden Globe'
	'Razzy'
	'Sundance'
);


CREATE TABLE movies (
  user_id   integer,
  name      varchar(40),
  director  varchar(40),
  awards    award[]
);
