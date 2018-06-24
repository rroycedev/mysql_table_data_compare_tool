<?php
require "lib/DatabaseTable.php";
require "lib/TableDataComparer.php";
require "lib/ConfigParser.php";

echo "******************************************\n";
echo "*   Database Table Data Compare Uility   *\n";
echo "*                                        *\n";
echo "*      Copyright@2018 TransUnion SRG     *\n";
echo "******************************************\n";

$shortOpts = ""; 
$longOpts = array("seedhost:", "desthost:", "schema::");
$options = getopt($shortOpts, $longOpts);

if (!array_key_exists("seedhost", $options)) {
	echo "You must specify the argument '--seedhost'\n";
	Syntax($argv[0]);
}

$seedHost = $options["seedhost"];

if (!array_key_exists("desthost", $options)) {
        echo "You must specify the argument '--desthost'\n";
        Syntax($argv[0]);
}

$destHost = $options["desthost"];

$schemaToCompare = "";

if (array_key_exists("schema", $options)) {
	$schemaToCompare = $options["schema"];
}

$configParser = new ConfigParser();

$configParser->parseConfig("/etc/mysql/dbcomparedata.ini");

$adminConfig  = $configParser->getConfig("admin");
$serverConfig = $configParser->getConfig("dbserver");

echo "Comparing seed host [$seedHost] with [$destHost] " . ($schemaToCompare == "" ? "for all schemas" : "for schema $schemaToCompare") . "\n";

$databaseTable = new DatabaseTable();

try {
	$adminConn = $databaseTable->Connect($adminConfig["dbhost"], $adminConfig["dbuser"], $adminConfig["dbpswd"], $adminConfig["dbname"]);

	$seedConn = $databaseTable->Connect($seedHost, $serverConfig["dbuser"], $serverConfig["dbpswd"], "");
	$databaseTable->query($seedConn, "set sql_log_bin=0");

	$destConn = $databaseTable->Connect($destHost, "rroyce", "mcdoodle22", "");
        $databaseTable->query($destConn, "set sql_log_bin=0");
}
catch(Exception $ex) {
	die($ex->getMessage() . "\n");
}

echo "Connected successfully to admin host: " . $adminConfig["dbhost"] . "\n";
echo "Connected successfully to seed host: $seedHost \n";
echo "Connected successfully to dest host: $destHost \n";

$tableDataComparer = new TableDataComparer(	array("hostname" => $adminConfig["dbhost"], "dbname" => $adminConfig["dbname"], "conn" => $adminConn), 
						array("hostname" => $seedHost, "conn" => $seedConn), 
						array("hostname" => $destHost, "conn" => $destConn));

if ($schemaToCompare == "") {
	$tableDataComparer->compareAllSchemas();
}
else {
	$tableDataComparer->compareSchema($schemaToCompare);
}

function Syntax($scriptName) {
	die("Syntax: php $scriptName --seedhost=<hostorip> --desthost=<hostorip>\n");
}

