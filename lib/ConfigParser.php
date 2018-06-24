<?php

class ConfigParser {
	private $_config = array(); 

	public function parseConfig($filename) {
		$this->_config = parse_ini_file($filename, true);
	}

	public function getConfig($section = "") {
		if ($section == "") {
			return $this->_config;
		}

		return $this->_config[$section];
	}
}


