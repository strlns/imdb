<?php

namespace Strlns\ImdbToyProject;

use mysqli;

final class Database
{
    public mysqli $mysql;
    public array $dbInfo;
    protected static ?self $instance = null;

    public static function getDatabase()
    {
        if (!self::$instance) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    private function __construct()
    {
        $dbString = $_ENV['DATABASE_URL'];
        preg_match(
            '#mysql://(?P<user>[^:]*):(?P<password>[^@]*)@(?P<host>[^/]*)/(?P<db>.*)#',
            $dbString,
            $matches
        );
        list($user, $password, $host, $db) = [
            $matches['user'],
            $matches['password'],
            $matches['host'],
            $matches['db'],
        ];
        $this->dbInfo = [
            'host' => $host,
            'db' => $db,
            'user' => $user,
        ];
        $this->mysql = new mysqli($host, $user, $password, $db);
    }

    public function __call(string $method, array $params)
    {
        return $this->mysql->{$method}(...$params);
    }
}
