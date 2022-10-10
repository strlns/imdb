<?php

namespace Strlns\ImdbToyProject;

use mysqli, mysqli_sql_exception;

class ImportImdbTsvFileCommand
{
    protected mysqli $db;
    protected string $file;
    protected int $offset;
    protected string $tableName;
    protected string $dbName;
    /*array of column names to use for PK. normally only one column. */
    protected array $primaryKey;
    protected $batch = [];
    protected $columnNames = [];
    protected int $nLines;
    protected int $nLinesToImport;
    protected int $batchSize;
    protected int $maxRows;

    /**@var resource $fileGzResource (gzipped file) */
    protected $fileGzResource;

    public function __construct(
        mysqli $db,
        string $dbName,
        string $file,
        ?int $offset = 0,
        ?int $batchSize = 512,
        ?int $maxRows = null
    ) {
        $this->db = $db;
        $this->dbName = $dbName;
        $this->offset = $offset;
        $this->batchSize = $batchSize;
        $this->maxRows = $maxRows ?? pow(2, 25); //~33.5 million
        $this->file = self::validateInputPath($file);
        $this->tableName = $this->getTableNameFromFileName();
        $this->sanitizeTableName();
    }

    public function run()
    {
        echo "\n\n*** IMPORTING FILE: $this->file***\n";
        $this->fileGzResource = gzopen($this->file, 'r');
        $this->countSourceLines();
        $this->prepareDataFromFile();
        $this->importRows();
        gzclose($this->fileGzResource);
    }

    protected function prepareDataFromFile()
    {
        $this->readColumnNames();
        $this->determinePrimaryKey();
        $this->seekOffset();
        $this->prepareTable();
        $this->prepareColumns();
    }

    protected function importRows(): void
    {
        $i = 0;
        $expectedColumnCount = count($this->columnNames);
        while (
            ++$i < $this->maxRows &&
            !gzeof($this->fileGzResource) &&
            ($row = static::getRow(gzgets($this->fileGzResource)))
        ) {
            $this->batch[] = $row;
            if (count($row) !== $expectedColumnCount) {
                echo "\n";
                var_dump($row);
                echo 'Error: wrong number of columns: ' . count($row) . "\n";
                continue;
            }
            if (count($this->batch) >= $this->batchSize) {
                $this->processBatch();
                $this->outputProgress($i);
                $this->batch = [];
            }
            if ($i === $this->maxRows - 1) {
                echo "\n STOPPING IMPORT, MAXIMUM NUMBER OF ROWS REACHED: $this->maxRows";
            }
        }
        $this->processBatch();
        $this->outputProgress($i);
        echo "\nImport complete.\n";
    }

    protected function getTableNameFromFileName(): string
    {
        return basename($this->file, '.tsv.gz');
    }

    protected function sanitizeTableName(): void
    {
        echo 'Table name: ' . $this->tableName . "\n";
        if (strpos($this->tableName, '.') !== -1) {
            echo "Table name contains a dot. This is allowed but might become inconvenient. We'll replace dots in the table name with underscores.\n";
            $this->tableName = str_replace('.', '_', $this->tableName);
        }
    }

    protected function countSourceLines(): void
    {
        echo "Counting source file lines...\n";
        $this->nLines = (int) exec(
            'zcat ' . escapeshellarg($this->file) . ' | wc -l'
        );
        echo "TSV File has $this->nLines lines.\n";

        $this->nLinesToImport = $this->nLines - $this->offset;
        echo "$this->nLinesToImport to import ($this->nLines total).\n";
    }

    protected function readColumnNames(): void
    {
        $this->columnNames = static::getRow(gzgets($this->fileGzResource));
    }

    protected function seekOffset(): void
    {
        rewind($this->fileGzResource);
        if ($this->offset > 0) {
            echo "Seeking in gzipped file for offset $this->offset...\n";
            $i = $this->offset;
            while (--$i > 0) {
                gzgets($this->fileGzResource);
            }
            echo 'Discarded ' . $this->offset . " lines\n";
        }
    }

    protected static function getRow($line)
    {
        $values = array_map('trim', explode("\t", $line));
        return $values;
    }

    protected static function validateInputPath(string $path)
    {
        list($path, $dir) = [realpath($path), realpath(IMDB_IMPORT_DIR)];
        if (strpos($path, $dir) !== 0) {
            die(
                "\nERROR: Please place your input files in a subdirectory of this script.\n"
            );
        }
        return $path;
    }

    protected static function flattenArray(array $array)
    {
        $return = [];
        array_walk_recursive($array, function ($a) use (&$return) {
            $return[] = $a;
        });
        return $return;
    }

    protected function getColumnDefinition(string $imdbFieldName)
    {
        return $imdbFieldName .
            ' ' .
            $this->sqlColumnType($imdbFieldName) .
            ' ' .
            ($this->isSingleColumnPrimaryKey($imdbFieldName)
                ? ' PRIMARY KEY'
                : ' NOT NULL');
    }

    protected function sqlColumnType(string $imdbFieldName): string
    {
        if ($this->isSingleColumnPrimaryKey($imdbFieldName)) {
            return 'CHAR(16)';
        }
        return preg_match('/^is[A-Z]/', $imdbFieldName)
            ? 'INT'
            : 'VARCHAR(255)';
    }

    protected static function sqlParamColumnType(string $imdbFieldName): string
    {
        return preg_match('/^is[A-Z]/', $imdbFieldName) ? 'i' : 's';
    }

    protected function isSingleColumnPrimaryKey(string $imdbFieldName): bool
    {
        return count($this->primaryKey) === 1 &&
            $this->primaryKey[0] === $imdbFieldName;
    }

    protected function isPartOfMultiColumnPrimaryKey(
        string $imdbFieldName
    ): bool {
        return count($this->primaryKey) > 1 &&
            in_array($imdbFieldName, $this->primaryKey, true);
    }

    protected function determinePrimaryKey(): void
    {
        $primaryKey = [];
        $constColumns = array_filter($this->columnNames, function (
            $columnName
        ) {
            return (bool) preg_match('/const$/', $columnName);
        });
        $constColumnCount = count($constColumns);
        if ($constColumnCount > 1) {
            echo 'WARNING: Multiple columns with a name ending in \'const\' found. Will use the first one as primary key.';
            $primaryKey = array_slice($constColumns, 0, 1);
        } elseif ($constColumnCount === 1) {
            $primaryKey = $constColumns;
        } else {
            $primaryKey = array_filter($this->columnNames, function (
                $columnName
            ) {
                return (bool) preg_match('/Id$/', $columnName);
            });
        }
        if (count($primaryKey) < 1) {
            throw new \Exception('Need a primary key.');
        }
        echo "\nUsing (" . implode(', ', $primaryKey) . ") as primary key.\n";
        $this->primaryKey = $primaryKey;
    }

    protected function prepareTable(): void
    {
        $stmt = $this->db->prepare("SELECT * FROM information_schema.tables 
            WHERE table_schema = '$this->dbName'
            AND table_name = '$this->tableName'
            LIMIT 1;");
        $stmt->execute();
        if ($stmt->get_result()->num_rows < 1) {
            echo "Table $this->tableName not found. It will be created.\n";
            $sql =
                'CREATE TABLE IF NOT EXISTS `' .
                $this->tableName .
                '` (' .
                $this->getCreateTableBody() .
                ') DEFAULT CHARSET utf8mb4;';
            $stmt = $this->db->prepare($sql);
            if ($stmt->execute()) {
                echo 'Table ' . $this->tableName . ' created';
            } else {
                throw new \Exception("Could not create $this->tableName");
            }
        } else {
            echo "Table $this->tableName found. \n";
        }
    }

    protected function getCreateTableBody(): string
    {
        return implode(
            ",\n",
            array_map(function ($columnName) {
                return $this->getColumnDefinition($columnName);
            }, $this->columnNames)
        ) .
            (count($this->primaryKey) > 1
                ? "\n PRIMARY KEY (" . implode(',', $this->primaryKey) . ')'
                : '');
    }

    protected function prepareColumns(): void
    {
        foreach ($this->columnNames as $columnName) {
            $sql = "SHOW COLUMNS FROM `$this->tableName` LIKE '$columnName';";
            $stmt = $this->db->prepare($sql);
            $stmt->execute();
            $numRows = $stmt->get_result()->num_rows;
            if ($numRows < 1) {
                echo 'Column ' .
                    $columnName .
                    ' not found. It will be created in table ' .
                    $this->tableName .
                    "\n";
                $sql =
                    "ALTER TABLE `$this->tableName` ADD COLUMN " .
                    static::getColumnDefinition($columnName) .
                    ';';
                $stmt = $this->db->prepare($sql);

                // $stmt->prepare('ALTER TABLE ? ADD COLUMN ' . $columnName . ' ' . getColumnDefinition($columnName) . ' NOT NULL');
                // $stmt->bind_param('s', $tableName);
                if ($stmt->execute()) {
                    echo 'Column ' . $columnName . ' created';
                } else {
                    throw new \Exception("Could not create $columnName");
                }
            }
        }
        echo "\nColumns checked.\n";
    }

    protected function outputProgress(int $line): void
    {
        echo "\rImporting line $line / $this->nLinesToImport (" .
            $line +
            $this->offset .
            " / $this->nLines total)";
    }

    protected function processBatch(): void
    {
        if (empty($this->batch)) {
            return;
        }
        $expectedColumnCount = count($this->columnNames);
        $placeholders = array_map(function ($row) use ($expectedColumnCount) {
            if (count($row) !== $expectedColumnCount) {
                var_dump($row);
                throw new \Exception(
                    'Unexpected number of columns: ' . count($row)
                );
            }
            return '(' . implode(',', array_fill(0, count($row), '?')) . ')';
        }, $this->batch);

        $sql =
            "REPLACE INTO `$this->tableName` (" .
            implode(', ', $this->columnNames) .
            ') VALUES ' .
            implode(',', $placeholders) .
            ';';
        // if (count($this->primaryKey) === 1) {
        //     $keyColumn = $this->primaryKey[0];
        //     $sql .= " ON DUPLICATE KEY UPDATE $keyColumn = VALUES($keyColumn);";
        // } else {
        //     $sql .=
        //         ' ON DUPLICATE KEY UPDATE ' . implode('', $this->primaryKey);
        // }
        try {
            $stmt = $this->db->prepare($sql);
            $types = str_repeat(
                implode(
                    '',
                    array_map(function ($columnName) {
                        return $this->sqlParamColumnType($columnName);
                    }, $this->columnNames)
                ),
                count($this->batch)
            );
            $values = static::flattenArray($this->batch);
            $stmt->bind_param($types, ...$values);
            $stmt->execute();
        } catch (mysqli_sql_exception $e) {
            echo "\n" . $sql . "\n";
            throw $e;
        }
    }
}
