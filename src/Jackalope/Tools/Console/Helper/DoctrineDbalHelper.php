<?php

namespace Jackalope\Tools\Console\Helper;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Helper\Helper;

/**
 * Helper class to make the session instance available to console command.
 *
 * @license http://www.apache.org/licenses Apache License Version 2.0, January 2004
 * @license http://opensource.org/licenses/MIT MIT License
 */
class DoctrineDbalHelper extends Helper
{
    /**
     * @var Connection
     */
    protected $connection;

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function getConnection()
    {
        return $this->connection;
    }

    public function getName()
    {
        return 'jackalope-doctrine-dbal';
    }
}
