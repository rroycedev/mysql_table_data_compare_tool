<?php

class DatabaseTable {

	public function Connect($host, $user, $pswd, $dbname) {

		$conn = mysqli_connect($host, $user, $pswd, $dbname);
		if (!$conn) {
        		throw new Exception ("Error connecting to $host : " . mysqli_connect_error($conn));
		}

		return $conn;	
	}

	public function query($conn, $sql) {
		echo "SQL> $sql\n";

		$res = mysqli_query($conn, $sql);

		if (!$res) {
			throw new Exception("Error executing query: " . mysqli_error($conn));
		}

		return $res;
	}

	public function fetch_assoc($res) {
		return mysqli_fetch_assoc($res);
	}

	public function getTableColumnInfo($conn, $tableSchema, $tableName) {
		$sql = "show columns from $tableSchema.$tableName";

		$res = $this->query($conn, $sql);

		$columns = array();

		while ($row = $this->fetch_assoc($res)) {
			$columns[] = $row;
		}

		return $columns;
	}
}

