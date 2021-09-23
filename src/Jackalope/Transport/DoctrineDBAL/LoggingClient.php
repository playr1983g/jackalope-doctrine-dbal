<?php

namespace Jackalope\Transport\DoctrineDBAL;

use Doctrine\DBAL\Connection;
use Jackalope\FactoryInterface;
use Jackalope\Query\Query;
use Jackalope\Transport\AbstractReadWriteLoggingWrapper;
use Jackalope\Transport\Logging\LoggerInterface;
use Jackalope\Transport\NodeTypeManagementInterface;
use Jackalope\Transport\QueryInterface as QueryTransport;
use Jackalope\Transport\TransactionInterface;
use Jackalope\Transport\WorkspaceManagementInterface;
use PHPCR\NamespaceException;
use PHPCR\Query\InvalidQueryException;

/**
 * Logging enabled wrapper for the Jackalope Doctrine DBAL client.
 *
 * @license http://www.apache.org/licenses Apache License Version 2.0, January 2004
 * @license http://opensource.org/licenses/MIT MIT License
 * @author Lukas Kahwe Smith <smith@pooteeweet.org>
 */

// PermissionInterface, VersioningInterface, LockingInterface, ObservationInterface
class LoggingClient extends AbstractReadWriteLoggingWrapper implements QueryTransport, NodeTypeManagementInterface, WorkspaceManagementInterface, TransactionInterface
{
    /**
     * @param Client          $transport A jackalope doctrine dbal client instance
     * @param LoggerInterface $logger    A logger instance
     */
    public function __construct(FactoryInterface $factory, Client $transport, LoggerInterface $logger)
    {
        parent::__construct($factory, $transport, $logger);
    }

    public function getConnection(): Connection
    {
        return $this->transport->getConnection();
    }

    /**
     * Configure whether to check if we are logged in before doing a request.
     *
     * Will improve error reporting at the cost of some round trips.
     */
    public function setCheckLoginOnServer($bool): void
    {
        $this->transport->setCheckLoginOnServer($bool);
    }

    /**
     * {@inheritDoc}
     *
     * @throws InvalidQueryException
     */
    public function query(Query $query): array
    {
        $this->logger->startCall(__FUNCTION__, func_get_args(), ['fetchDepth' => $this->transport->getFetchDepth()]);
        $result = $this->transport->query($query);
        $this->logger->stopCall();

        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function getSupportedQueryLanguages(): array
    {
        return $this->transport->getSupportedQueryLanguages();
    }

    /**
     * {@inheritDoc}
     *
     * @throws NamespaceException
     */
    public function registerNamespace($prefix, $uri): void
    {
        $this->transport->registerNamespace($prefix, $uri);
    }

    /**
     * {@inheritDoc}
     */
    public function unregisterNamespace($prefix): void
    {
        $this->transport->unregisterNamespace($prefix);
    }

    /**
     * {@inheritDoc}
     */
    public function registerNodeTypes($types, $allowUpdate): bool
    {
        return $this->transport->registerNodeTypes($types, $allowUpdate);
    }

    /**
     * {@inheritDoc}
     */
    public function createWorkspace($name, $srcWorkspace = null): void
    {
        $this->transport->createWorkspace($name, $srcWorkspace);
    }

    /**
     * {@inheritDoc}
     */
    public function deleteWorkspace($name): void
    {
        $this->transport->deleteWorkspace($name);
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction(): ?string
    {
        return $this->transport->beginTransaction();
    }

    /**
     * {@inheritDoc}
     */
    public function commitTransaction(): void
    {
        $this->transport->commitTransaction();
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackTransaction(): void
    {
        $this->transport->rollbackTransaction();
    }

    /**
     * {@inheritDoc}
     */
    public function setTransactionTimeout($seconds): void
    {
        $this->transport->setTransactionTimeout($seconds);
    }
}
