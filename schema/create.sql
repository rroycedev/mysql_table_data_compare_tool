CREATE TABLE `rc_compare_table_names` (
  `seed_hostname` varchar(60) NOT NULL,
  `schema_name` varchar(64) NOT NULL,
  `table_name` varchar(64) NOT NULL,
  `control_table_name` varchar(64) NOT NULL,
  PRIMARY KEY (`seed_hostname`,`schema_name`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 ;

