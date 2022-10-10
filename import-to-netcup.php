<?php

use Strlns\ImdbToyProject\ImportImdbTsvFileCommand;
use Strlns\ImdbToyProject\Database;

require __DIR__ . '/vendor/autoload.php';
define('IMDB_IMPORT_DIR', dirname(__DIR__));
$dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
$dotenv->safeLoad();

$opts = getopt('', ['offset::'], $restIndex);
if (empty($argv[$restIndex])) {
    // throw new \InvalidArgumentException(
    die("Please specify file.tsv.gz as first argument.\n");
}
$file = $argv[$restIndex];
$offset = (int) ($opts['offset'] ?? 0);

$database = Database::getDatabase();
$importer = new ImportImdbTsvFileCommand(
    $database->mysql,
    $database->dbInfo['db'],
    $file,
    $offset
);
$importer->run();
