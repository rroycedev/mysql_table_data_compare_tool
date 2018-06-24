<?php

class TableDataComparer extends DatabaseTable {
	private $_seedConnectionInfo = null;
	private $_destConnectionInfo = null;

	public function __construct($adminConnectionInfo, $seedConnectionInfo, $destConnectionInfo) {
		$this->_adminConnectionInfo = $adminConnectionInfo;
		$this->_seedConnectionInfo  = $seedConnectionInfo;
		$this->_destConnectionInfo  = $destConnectionInfo;
	}

	public function compareSchema($schemaName) {
		echo "Comparing schema $schemaName ....\n";

		$seedConn = $this->_seedConnectionInfo["conn"];
		$destConn = $this->_destConnectionInfo["conn"];

		$seedHostname = $this->_seedConnectionInfo["hostname"];
	
		$sql = "select table_name from information_schema.tables where table_schema = '$schemaName' and table_name not like 'temp_rc_%' order by table_name";

		$res = $this->query($seedConn, $sql);

		$tableNames = array();

		while ($row = $this->fetch_assoc($res)) {
			$tableNames[] = $row["table_name"];
		}

		foreach ($tableNames as $tableName) {
			echo "Comparing table $schemaName.$tableName ....\n";

			$tableColumnsInfo = $this->getTableColumnInfo($seedConn, $schemaName, $tableName);

			$isAutoIncrement = false;
			$primaryKeyColName = "";
			$found = false;

			foreach ($tableColumnsInfo as $column) {
				if ($column["Key"] == "PRI") {
					print_r($column);
					$found = true;
					$primaryKeyColName = $column["Field"];
					$primaryKeyColDataType = $column["Type"];

					if ($column["Extra"] == "auto_increment") {
						$isAutoIncrement = true;
					}
					else {
						$isAutoIncrement = false;
					}

					 break;
				}
			}

			if (!strlen($primaryKeyColName)) {
				echo "No primary key found for table $schemaName.$tableName.  Skipping...\n";
				continue;
			}

                        echo "Primary Key Column [$primaryKeyColName]  Is Auto Increment [" . ($isAutoIncrement ? "True" : "False") . "]\n";

			if ($isAutoIncrement) {
				$tempTableName = $this->createTemporaryComparisonTable($seedHostname, $schemaName, $tableName, $primaryKeyColName, $primaryKeyColDataType);

				$highestSeedAutoIncId = $this->getMaxTableAutoIncId($seedConn, $schemaName, $tableName, $primaryKeyColName);
				$highestDestAutoIncId = $this->getMaxTableAutoIncId($destConn, $schemaName, $tableName, $primaryKeyColName);	
	
				$maxIdToUse = min($highestSeedAutoIncId, $highestDestAutoIncId);
			
				$allPrimaryKeyAutoIncIds = $this->getAllTableAutoIncrementIds($seedConn, $schemaName, $tableName, $primaryKeyColName, $maxIdToUse);

				foreach ($allPrimaryKeyAutoIncIds as $id) {
                                        $md5Value = $this->getRowHash($seedConn, $schemaName, $tableName, $primaryKeyColName, $id, $tableColumnsInfo);
                                        
                                        $this->insertRowHash($tempTableName, $primaryKeyColName, $id, "seed", $md5Value);
				}

                                foreach ($allPrimaryKeyAutoIncIds as $id) {
					$md5Value = $this->getRowHash($destConn, $schemaName, $tableName, $primaryKeyColName, $id, $tableColumnsInfo);

                                        $this->insertRowHash($tempTableName, $primaryKeyColName, $id, "dest", $md5Value);
                                }

				$this->deleteRowsTheSame($tempTableName);

//				$diff = $this->getTableDifferences($seedConn, $schemaName, $primaryKeyColName, $tempTableName);
			}
		}
	}

	private function deleteRowsTheSame($tempTableName) {
		$sql = "delete from $tempTableName where seed_md5 = dest_md5";

		$this->query($this->_adminConnectionInfo["conn"], $sql);
	}

	private function getTableDifferences($seedConn, $schemaName, $primaryKeyColName, $tempTableName) {
		$sql = "select $primaryKeyColName from $schemaName.$tempTableName where seed_md5 is null union all select $primaryKeyColName from $schemaName.$tempTableName where dest_md5 is null union all select $primaryKeyColName from $schemaName.$tempTableName where seed_md5 is not null and dest_md5 is not null and seed_md5 <> dest_md5";

		$res = $this->query($seedConn, $sql);

		while ($row = $this->fetch_assoc($res)) {
			echo "DIFFERENT: " . $row[$primaryKeyColName] . "\n";
		}
	}

	private function getRowHash($conn, $schemaName, $tableName, $primaryKeyColName, $id, $tableColumnsInfo) {
                $hashFormula = "MD5(CONCAT_WS(','";

                foreach ($tableColumnsInfo as $column) {
                        $hashFormula .= ", src." . $column["Field"];
                }

                $hashFormula .= "))";

                $sql = "SELECT $hashFormula as md5hash FROM $schemaName.$tableName src where src.$primaryKeyColName = $id";

                $res = $this->query($conn, $sql);

                $row = $this->fetch_assoc($res);

                return $row["md5hash"];
	}
	
	private function insertRowHash($tempTableName, $primaryKeyColName, $id, $src, $md5Value) {
		if ($src == "seed") {
			$sql = "insert into $tempTableName ($primaryKeyColName, seed_md5, dest_md5) VALUES($id, '$md5Value', NULL) ON DUPLICATE KEY UPDATE seed_md5 = '$md5Value'";
		}
		else {
                        $sql = "insert into $tempTableName ($primaryKeyColName, seed_md5, dest_md5) VALUES($id, '$md5Value', NULL) ON DUPLICATE KEY UPDATE dest_md5 = '$md5Value'";
                }

		$this->query($this->_adminConnectionInfo["conn"], $sql);
	}

	private function getControlTableName($seedHostname, $schemaName, $tableName) {
		$sql = "select * 
			from rc_compare_table_names 
			where seed_hostname = '$seedHostname' and
				schema_name = '$schemaName' and
				table_name = '$tableName'
			";

		$res = $this->query($this->_adminConnectionInfo["conn"], $sql);

		$row = $this->fetch_assoc($res);

		if ($row) {
			return $row["control_table_name"];
		}

		$controlTableName = "rc_" . md5($seedHostname . "-" . $schemaName . "-" . $tableName);

		$sql = "insert into rc_compare_table_names (seed_hostname, schema_name, table_name, control_table_name) values('$seedHostname', '$schemaName', '$tableName', '$controlTableName')";

		$conn = $this->_adminConnectionInfo["conn"];

		$this->query($conn, $sql);

		return $controlTableName;
	}

	private function createTemporaryComparisonTable($seedHostname, $schemaName, $tableName, $primaryKeyColName, $primaryKeyColDataType) {
		$tempTableName = $this->getControlTableName($seedHostname, $schemaName, $tableName);

		$sql = "CREATE TABLE IF NOT EXISTS $tempTableName 
			(
				$primaryKeyColName $primaryKeyColDataType,
				seed_md5 CHAR(32),
				dest_md5 CHAR(32),
				PRIMARY KEY ($primaryKeyColName),
				KEY (seed_md5),
				KEY (dest_md5)
			)Engine=InnoDB";

		$this->query($this->_adminConnectionInfo["conn"], $sql);

		return $tempTableName;
	}
			
	public function compareAllSchemas() {
		echo "Comparing all schemas ....\n";
	}

	private function getMaxTableAutoIncId($conn, $schemaName, $tableName, $primaryKeyColName) {
		$sql = "select IFNULL(max($primaryKeyColName), 0) as max_id from $schemaName.$tableName";

		$res = $this->query($conn, $sql);

		$row = $this->fetch_assoc($res);

		return $row["max_id"];
	}

	private function getAllTableAutoIncrementIds($conn, $schemaName, $tableName, $primaryKeyColName, $maxIdToUse) {
		$limit = 100000;
		$offset = 0;

		$rows = array();

		while (true) {
			$sql = "select $primaryKeyColName from $schemaName.$tableName where $primaryKeyColName <= $maxIdToUse order by $primaryKeyColName limit $limit OFFSET $offset";

			$res = $this->query($conn, $sql);
	
			$rowsFetched = 0;

			while ($row = $this->fetch_assoc($res)) {
				$rowsFetched++;
				$rows[] = $row[$primaryKeyColName];
			}

			if ($rowsFetched < $limit) {
				break;
			}	
		}

		return $rows;
	}

}

