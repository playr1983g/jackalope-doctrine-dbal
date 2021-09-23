<?php

namespace Jackalope\Transport\DoctrineDBAL;

use ArrayObject;
use Closure;
use DateTime;
use DateTimeZone;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDO\Connection as PDOConnection;
use Doctrine\DBAL\Exception as DBALException;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQL94Platform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Doctrine\DBAL\Statement;
use DOMDocument;
use DOMXPath;
use Exception;
use InvalidArgumentException;
use Jackalope\FactoryInterface;
use Jackalope\Node;
use Jackalope\NodeType\NodeProcessor;
use Jackalope\NodeType\NodeTypeDefinition;
use Jackalope\NodeType\NodeTypeManager;
use Jackalope\NotImplementedException;
use Jackalope\Property;
use Jackalope\Query\QOM\QueryObjectModelFactory;
use Jackalope\Query\Query;
use Jackalope\Transport\BaseTransport;
use Jackalope\Transport\DoctrineDBAL\Query\QOMWalker;
use Jackalope\Transport\DoctrineDBAL\XmlParser\XmlToPropsParser;
use Jackalope\Transport\MoveNodeOperation;
use Jackalope\Transport\NodeTypeManagementInterface;
use Jackalope\Transport\QueryInterface as QueryTransport;
use Jackalope\Transport\StandardNodeTypes;
use Jackalope\Transport\TransactionInterface;
use Jackalope\Transport\WorkspaceManagementInterface;
use Jackalope\Transport\WritingInterface;
use PDO;
use PHPCR\AccessDeniedException;
use PHPCR\CredentialsInterface;
use PHPCR\ItemExistsException;
use PHPCR\ItemNotFoundException;
use PHPCR\LoginException;
use PHPCR\NamespaceException;
use PHPCR\NamespaceRegistryInterface as NS;
use PHPCR\NodeType\ConstraintViolationException;
use PHPCR\NodeType\NodeDefinitionInterface;
use PHPCR\NodeType\NodeTypeExistsException;
use PHPCR\NodeType\NoSuchNodeTypeException;
use PHPCR\NodeType\PropertyDefinitionInterface;
use PHPCR\NoSuchWorkspaceException;
use PHPCR\PathNotFoundException;
use PHPCR\PropertyType;
use PHPCR\Query\InvalidQueryException;
use PHPCR\Query\QOM\QueryObjectModelConstantsInterface as QOM;
use PHPCR\Query\QOM\QueryObjectModelInterface;
use PHPCR\Query\QOM\SelectorInterface;
use PHPCR\Query\QueryInterface;
use PHPCR\ReferentialIntegrityException;
use PHPCR\RepositoryException;
use PHPCR\RepositoryInterface;
use PHPCR\SimpleCredentials;
use PHPCR\UnsupportedRepositoryOperationException;
use PHPCR\Util\PathHelper;
use PHPCR\Util\QOM\Sql2ToQomQueryConverter;
use PHPCR\Util\UUIDHelper;
use PHPCR\Util\ValueConverter;
use stdClass;

/**
 * Class to handle the communication between Jackalope and RDBMS via Doctrine DBAL.
 *
 * @license http://www.apache.org/licenses Apache License Version 2.0, January 2004
 * @license http://opensource.org/licenses/MIT MIT License
 * @author Benjamin Eberlei <kontakt@beberlei.de>
 * @author Lukas Kahwe Smith <smith@pooteeweet.org>
 */
class Client extends BaseTransport implements QueryTransport, WritingInterface, WorkspaceManagementInterface, NodeTypeManagementInterface, TransactionInterface
{
    /**
     * SQlite can only handle a maximum of 999 parameters inside an IN statement
     * see https://github.com/jackalope/jackalope-doctrine-dbal/pull/149/files#diff-a3a0165ed79ca1ba3513ec5ecd59ec56R707.
     */
    private const SQLITE_MAXIMUM_IN_PARAM_COUNT = 999;
    private const DBAL2 = 'DBAL2';
    private const DBAL3 = 'DBAL3';
    private string $dbalVersion;

    /**
     * The factory to instantiate objects.
     */
    private FactoryInterface $factory;

    private ValueConverter $valueConverter;

    private ?Connection $conn;

    private Closure $uuidGenerator;

    private bool $loggedIn = false;

    private ?SimpleCredentials $credentials;

    protected string $workspaceName;

    private array $nodeIdentifiers = [];

    private NodeTypeManager $nodeTypeManager;

    protected bool $inTransaction = false;

    /**
     * Check if an initial request on login should be send to check if repository exists
     * This is according to the JCR specifications and set to true by default.
     *
     * @see setCheckLoginOnServer
     */
    private bool $checkLoginOnServer = true;

    /**
     * Using an ArrayObject here so that we can pass this into the NodeProcessor by reference more elegantly.
     */
    protected ?ArrayObject $namespaces = null;

    /**
     * @var array|null the namespaces at initial state when making changes to the namespaces, in case of rollback
     */
    private ?array $originalNamespaces = null;

    /**
     * The core namespaces defined in JCR.
     */
    private array $coreNamespaces = [
        NS::PREFIX_EMPTY => NS::NAMESPACE_EMPTY,
        NS::PREFIX_JCR => NS::NAMESPACE_JCR,
        NS::PREFIX_NT => NS::NAMESPACE_NT,
        NS::PREFIX_MIX => NS::NAMESPACE_MIX,
        NS::PREFIX_XML => NS::NAMESPACE_XML,
        NS::PREFIX_SV => NS::NAMESPACE_SV,
    ];

    private ?string $sequenceNodeName = null;

    private ?string $sequenceTypeName = null;

    private array $referencesToUpdate = [];

    private array $referenceTables = [
        PropertyType::REFERENCE => 'phpcr_nodes_references',
        PropertyType::WEAKREFERENCE => 'phpcr_nodes_weakreferences',
    ];

    private array $referencesToDelete = [];

    private bool $connectionInitialized = false;

    private NodeProcessor $nodeProcessor;

    private ?string $caseSensitiveEncoding = null;

    public function __construct(FactoryInterface $factory, Connection $conn)
    {
        $this->factory = $factory;
        $this->valueConverter = $this->factory->get(ValueConverter::class);
        $this->conn = $conn;
    }

    /**
     * @TODO: move to "SqlitePlatform" and rename to "registerExtraFunctions"?
     */
    private function registerSqliteFunctions(PDO $sqliteConnection): void
    {
        $sqliteConnection->sqliteCreateFunction(
            'EXTRACTVALUE',
            function ($string, $expression) {
                if (null === $string) {
                    return null;
                }

                $dom = new DOMDocument('1.0', 'UTF-8');
                $dom->loadXML($string);
                $xpath = new DOMXPath($dom);
                $list = $xpath->evaluate($expression);

                if (!is_object($list)) {
                    return $list;
                }

                // @TODO: don't know if there are expressions returning more then one row
                if ($list->length > 0) {
                    // @TODO: why it can happen that we do not have a type? https://github.com/phpcr/phpcr-api-tests/pull/132
                    $type = is_object($list->item(0)->parentNode->attributes->getNamedItem('type')) ? $list->item(
                        0
                    )->parentNode->attributes->getNamedItem('type')->value : null;
                    $content = $list->item(0)->textContent;

                    switch ($type) {
                        case 'long':
                            return (int) $content;

                        case 'double':
                            return (float) $content;

                        default:
                            return $content;
                    }
                }

                // @TODO: don't know if return value is right
                return null;
            },
            2
        );

        $sqliteConnection->sqliteCreateFunction(
            'CONCAT',
            function () {
                return implode('', func_get_args());
            }
        );
    }

    public function getConnection(): Connection
    {
        $this->initConnection();

        return $this->conn;
    }

    /**
     * Set the UUID generator to use. If not set, the phpcr-utils UUIDHelper
     * will be used.
     */
    public function setUuidGenerator(Closure $generator): void
    {
        $this->uuidGenerator = $generator;
    }

    /**
     * @return callable a uuid generator function
     */
    private function getUuidGenerator(): callable
    {
        if (!isset($this->uuidGenerator)) {
            $this->uuidGenerator = static function (): string {
                return UUIDHelper::generateUUID();
            };
        }

        return $this->uuidGenerator;
    }

    private function generateUuid(): string
    {
        // php 5.3 compatibility, no direct execution of this function.
        $g = $this->getUuidGenerator();

        return $g();
    }

    /**
     * {@inheritDoc}
     *
     * @throws NotImplementedException
     */
    public function createWorkspace($name, $srcWorkspace = null)
    {
        if (null !== $srcWorkspace) {
            throw new NotImplementedException('Creating workspace as clone of existing workspace not supported');
        }

        if ($this->workspaceExists($name)) {
            throw new RepositoryException("Workspace '$name' already exists");
        }

        try {
            $this->getConnection()->insert('phpcr_workspaces', ['name' => $name]);
        } catch (Exception $e) {
            throw new RepositoryException("Couldn't create Workspace '$name': ".$e->getMessage(), 0, $e);
        }

        $this->getConnection()->insert(
            'phpcr_nodes',
            [
                'path' => '/',
                'parent' => '',
                'workspace_name' => $name,
                'identifier' => $this->generateUuid(),
                'type' => 'nt:unstructured',
                'local_name' => '',
                'namespace' => '',
                'props' => '<?xml version="1.0" encoding="UTF-8"?>
<sv:node xmlns:'.NS::PREFIX_MIX.'="'.NS::NAMESPACE_MIX.'" xmlns:'.NS::PREFIX_NT.'="'.NS::NAMESPACE_NT.'" xmlns:xs="http://www.w3.org/2001/XMLSchema" xmlns:'.NS::PREFIX_JCR.'="'.NS::NAMESPACE_JCR.'" xmlns:'.NS::PREFIX_SV.'="'.NS::NAMESPACE_SV.'" xmlns:rep="internal" />',
                // TODO compute proper value
                'depth' => 0,
            ]
        );
    }

    /**
     * {@inheritDoc}
     */
    public function deleteWorkspace($name)
    {
        if (!$this->workspaceExists($name)) {
            throw new RepositoryException("Workspace '$name' cannot be deleted as it does not exist");
        }

        try {
            $this->getConnection()->delete('phpcr_workspaces', ['name' => $name]);
        } catch (Exception $e) {
            throw new RepositoryException("Couldn't delete workspace '$name': ".$e->getMessage(), 0, $e);
        }

        try {
            $this->getConnection()->delete('phpcr_nodes', ['workspace_name' => $name]);
        } catch (Exception $e) {
            throw new RepositoryException("Couldn't delete nodes in workspace '$name': ".$e->getMessage(), 0, $e);
        }

        try {
            $this->getConnection()->delete('phpcr_binarydata', ['workspace_name' => $name]);
        } catch (Exception $e) {
            throw new RepositoryException(
                "Couldn't delete binary data in workspace '$name': ".$e->getMessage(),
                0,
                $e
            );
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws NotImplementedException
     * @throws AccessDeniedException
     * @throws UnsupportedRepositoryOperationException
     */
    public function login(CredentialsInterface $credentials = null, $workspaceName = null)
    {
        $this->credentials = $credentials;

        $this->workspaceName = $workspaceName ?: 'default';

        if (!$this->checkLoginOnServer) {
            return $this->workspaceName;
        }

        if (!$this->workspaceExists($this->workspaceName)) {
            if ('default' !== $this->workspaceName) {
                throw new NoSuchWorkspaceException("Requested workspace: '{$this->workspaceName}'");
            }

            // Create default workspace if it not exists
            $this->createWorkspace($this->workspaceName);
        }

        $this->loggedIn = true;

        return $this->workspaceName;
    }

    /**
     * {@inheritDoc}
     */
    public function logout()
    {
        if ($this->loggedIn) {
            $this->loggedIn = false;
            $this->conn->close();
            $this->conn = null;
        }
    }

    /**
     * Configure whether to check if we are logged in before doing a request.
     *
     * Will improve error reporting at the cost of some round trips.
     */
    public function setCheckLoginOnServer($bool)
    {
        $this->checkLoginOnServer = $bool;
    }

    /**
     * This will control the collate which is being used on MySQL when querying nodes. It will be autodetected by just
     * appending _bin to the current charset, which is good enough in most cases.
     *
     * @param string $encoding
     */
    public function setCaseSensitiveEncoding($encoding)
    {
        $this->caseSensitiveEncoding = $encoding;
    }

    /**
     * Returns the collate which is being used on MySQL when querying nodes.
     */
    private function getCaseSensitiveEncoding(): string
    {
        if (null !== $this->caseSensitiveEncoding) {
            return $this->caseSensitiveEncoding;
        }

        $params = $this->conn->getParams();
        $charset = $params['charset'] ?? 'utf8';
        if (isset($params['defaultTableOptions']['collate'])) {
            return $this->caseSensitiveEncoding = $params['defaultTableOptions']['collate'];
        }

        return $this->caseSensitiveEncoding = 'binary' === $charset ? $charset : $charset.'_bin';
    }

    protected function workspaceExists($workspaceName): bool
    {
        $query = 'SELECT 1 FROM phpcr_workspaces WHERE name = ?';
        try {
            return 0 < count($this->getConnection()->fetchFirstColumn($query, [$workspaceName]));
        } catch (Exception $e) {
            if ($e instanceof DBALException) {
                if (1045 === $e->getCode()) {
                    throw new LoginException('Access denied with your credentials: '.$e->getMessage());
                }
                if ('42S02' === $e->getCode()) {
                    throw new RepositoryException(
                        'You did not properly set up the database for the repository. See README.md for more information. Message from backend: '.$e->getMessage(
                        )
                    );
                }

                throw new RepositoryException('Unexpected error talking to the backend: '.$e->getMessage(), 0, $e);
            }

            throw $e;
        }
    }

    /**
     * Ensure that we are currently logged in, executing the login in case we
     * did lazy login.
     *
     * @throws NotImplementedException
     * @throws AccessDeniedException
     * @throws LoginException
     * @throws NoSuchWorkspaceException
     * @throws UnsupportedRepositoryOperationException
     * @throws RepositoryException                     if this transport is not logged in
     */
    protected function assertLoggedIn(): void
    {
        if ($this->loggedIn) {
            return;
        }
        if (!$this->checkLoginOnServer && $this->workspaceName) {
            $this->checkLoginOnServer = true;
            if ($this->login($this->credentials, $this->workspaceName)) {
                return;
            }
        }

        throw new RepositoryException('You need to be logged in for this operation');
    }

    /**
     * {@inheritDoc}
     */
    public function getRepositoryDescriptors(): array
    {
        return [
            RepositoryInterface::IDENTIFIER_STABILITY => RepositoryInterface::IDENTIFIER_STABILITY_INDEFINITE_DURATION,
            RepositoryInterface::REP_NAME_DESC => 'jackalope_doctrine_dbal',
            RepositoryInterface::REP_VENDOR_DESC => 'Jackalope Community',
            RepositoryInterface::REP_VENDOR_URL_DESC => 'http://github.com/jackalope',
            RepositoryInterface::REP_VERSION_DESC => '1.1.0-DEV',
            RepositoryInterface::SPEC_NAME_DESC => 'Content Repository for PHP',
            RepositoryInterface::SPEC_VERSION_DESC => '2.1',
            RepositoryInterface::NODE_TYPE_MANAGEMENT_AUTOCREATED_DEFINITIONS_SUPPORTED => true,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_INHERITANCE => RepositoryInterface::NODE_TYPE_MANAGEMENT_INHERITANCE_SINGLE,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_MULTIPLE_BINARY_PROPERTIES_SUPPORTED => true,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_MULTIVALUED_PROPERTIES_SUPPORTED => true,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_ORDERABLE_CHILD_NODES_SUPPORTED => true,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_OVERRIDES_SUPPORTED => false,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_PRIMARY_ITEM_NAME_SUPPORTED => true,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_PROPERTY_TYPES => true,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_RESIDUAL_DEFINITIONS_SUPPORTED => false,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_SAME_NAME_SIBLINGS_SUPPORTED => false,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_UPDATE_IN_USE_SUPPORTED => false,
            RepositoryInterface::NODE_TYPE_MANAGEMENT_VALUE_CONSTRAINTS_SUPPORTED => false,
            RepositoryInterface::OPTION_ACCESS_CONTROL_SUPPORTED => false,
            RepositoryInterface::OPTION_ACTIVITIES_SUPPORTED => false,
            RepositoryInterface::OPTION_BASELINES_SUPPORTED => false,
            RepositoryInterface::OPTION_JOURNALED_OBSERVATION_SUPPORTED => false,
            RepositoryInterface::OPTION_LIFECYCLE_SUPPORTED => false,
            RepositoryInterface::OPTION_LOCKING_SUPPORTED => false,
            RepositoryInterface::OPTION_NODE_AND_PROPERTY_WITH_SAME_NAME_SUPPORTED => true,
            RepositoryInterface::OPTION_NODE_TYPE_MANAGEMENT_SUPPORTED => true,
            RepositoryInterface::OPTION_OBSERVATION_SUPPORTED => false,
            RepositoryInterface::OPTION_RETENTION_SUPPORTED => false,
            RepositoryInterface::OPTION_SHAREABLE_NODES_SUPPORTED => false,
            RepositoryInterface::OPTION_SIMPLE_VERSIONING_SUPPORTED => false,
            RepositoryInterface::OPTION_TRANSACTIONS_SUPPORTED => true,
            RepositoryInterface::OPTION_UNFILED_CONTENT_SUPPORTED => true,
            RepositoryInterface::OPTION_UPDATE_MIXIN_NODETYPES_SUPPORTED => true,
            RepositoryInterface::OPTION_UPDATE_PRIMARY_NODETYPE_SUPPORTED => true,
            RepositoryInterface::OPTION_VERSIONING_SUPPORTED => false,
            RepositoryInterface::OPTION_WORKSPACE_MANAGEMENT_SUPPORTED => true,
            RepositoryInterface::OPTION_XML_EXPORT_SUPPORTED => true,
            RepositoryInterface::OPTION_XML_IMPORT_SUPPORTED => true,
            RepositoryInterface::QUERY_FULL_TEXT_SEARCH_SUPPORTED => true,
            RepositoryInterface::QUERY_CANCEL_SUPPORTED => false,
            RepositoryInterface::QUERY_JOINS => RepositoryInterface::QUERY_JOINS_NONE,
            RepositoryInterface::QUERY_LANGUAGES => [QueryInterface::JCR_SQL2, QueryInterface::JCR_JQOM],
            RepositoryInterface::QUERY_STORED_QUERIES_SUPPORTED => false,
            RepositoryInterface::WRITE_SUPPORTED => true,
        ];
    }

    /**
     * Get the registered namespace prefixes.
     */
    private function getNamespacePrefixes(): array
    {
        return array_keys($this->getNamespaces());
    }

    /**
     * {@inheritDoc}
     */
    public function getNamespaces(): array
    {
        return (array) $this->getNamespacesObject();
    }

    /**
     * Return the namespaces of the current session as a referenceable ArrayObject.
     */
    private function getNamespacesObject(): ArrayObject
    {
        if (null === $this->namespaces) {
            $query = 'SELECT prefix, uri FROM phpcr_namespaces';
            $result = $this->getConnection()->executeQuery($query);
            $columns = $result->fetchAllNumeric();

            $namespaces = array_column($columns, 1, 0);
            $namespaces += $this->coreNamespaces;

            $this->setNamespaces($namespaces);
        }

        return $this->namespaces;
    }

    /**
     * Set the namespaces property to an \ArrayObject instance.
     *
     * @param array|ArrayObject $namespaces
     */
    protected function setNamespaces($namespaces): void
    {
        if ($this->namespaces instanceof ArrayObject) {
            $this->namespaces->exchangeArray($namespaces);
        } else {
            $this->namespaces = new ArrayObject($namespaces);
        }
    }

    /**
     * Executes an UPDATE on DBAL while ensuring that we never try to send more than 999 parameters to SQLite.
     *
     * @throws DBALException
     */
    private function executeChunkedUpdate(string $query, array $params): void
    {
        $types = [Connection::PARAM_INT_ARRAY];

        if ($this->getConnection()->getDatabasePlatform() instanceof SqlitePlatform) {
            foreach (array_chunk($params, self::SQLITE_MAXIMUM_IN_PARAM_COUNT) as $chunk) {
                $this->getConnection()->executeQuery($query, [$chunk], $types);
            }
        } else {
            $this->getConnection()->executeQuery($query, [$params], $types);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws NoSuchWorkspaceException
     * @throws RepositoryException
     * @throws PathNotFoundException
     * @throws ItemExistsException
     * @throws DBALException
     * @throws InvalidArgumentException
     */
    public function copyNode($srcAbsPath, $dstAbsPath, $srcWorkspace = null): void
    {
        $this->assertLoggedIn();

        if (null !== $srcWorkspace && !$this->workspaceExists($srcWorkspace)) {
            throw new NoSuchWorkspaceException("Source workspace '$srcWorkspace' does not exist.");
        }
        $srcWorkspace = $srcWorkspace ?: $this->workspaceName;

        PathHelper::assertValidAbsolutePath($dstAbsPath, true, true, $this->getNamespacePrefixes());

        $srcNodeId = $this->getSystemIdForNode($srcAbsPath, $srcWorkspace);
        if (!$srcNodeId) {
            throw new PathNotFoundException("Source path '$srcAbsPath' not found");
        }

        if ($this->getSystemIdForNode($dstAbsPath)) {
            throw new ItemExistsException("Cannot copy to destination path '$dstAbsPath' that already exists.");
        }

        if (!$this->getSystemIdForNode(PathHelper::getParentPath($dstAbsPath))) {
            throw new PathNotFoundException("Parent of the destination path '".$dstAbsPath."' has to exist.");
        }

        // Algorithm:
        // 1. Select all nodes with path $srcAbsPath."%" and iterate them
        // 2. create a new node with path $dstAbsPath + leftovers, with a new uuid. Save old => new uuid
        // 3. copy all properties from old node to new node
        // 4. if a reference is in the properties, either update the uuid based on the map if its inside the copied graph or keep it.
        // 5. "May drop mixin types"

        $query = 'SELECT * FROM phpcr_nodes WHERE (path = ? OR path LIKE ?) AND workspace_name = ? ORDER BY depth, sort_order';
        $stmt = $this->getConnection()->executeQuery($query, [$srcAbsPath, $srcAbsPath.'/%', $srcWorkspace]);
        $rows = $stmt->fetchAllAssociative();

        $uuidMap = [];
        $resultSetUuids = [];

        // first iterate and build up an array of all the UUIDs in the result set
        foreach ($rows as $row) {
            $resultSetUuids[$row['identifier']] = $row['path'];
        }

        // array of references to remap within the copied tree
        $referenceElsToRemap = [];

        // array references that will need updating in the database
        $referencesToUpdate = [];

        foreach ($rows as $row) {
            $newPath = str_replace($srcAbsPath, $dstAbsPath, $row['path']);

            $stringDom = new DOMDocument('1.0', 'UTF-8');
            $stringDom->loadXML($row['props']);
            $numericalDom = null;
            if ($row['numerical_props']) {
                $numericalDom = new DOMDocument('1.0', 'UTF-8');
                $numericalDom->loadXML($row['numerical_props']);
            }

            $propsData = [
                'stringDom' => $stringDom,
                'numericalDom' => $numericalDom,
                'references' => [],
            ];

            $xpath = new DOMXpath($stringDom);
            $referenceEls = $xpath->query(
                './/sv:property[@sv:type="reference" or @sv:type="Reference" or @sv:type="weakreference" or @sv:type="WeakReference"]'
            );

            $references = [];
            foreach ($referenceEls as $referenceEl) {
                $propName = $referenceEl->getAttribute('sv:name');
                $values = [];
                foreach ($xpath->query('./sv:value', $referenceEl) as $valueEl) {
                    $values[] = $valueEl->nodeValue;
                }

                $references[$propName] = [
                    'type' => PropertyType::valueFromName($referenceEl->getAttribute('sv:type')),
                    'values' => $values,
                ];

                if (isset($resultSetUuids[$referenceEl->nodeValue])) {
                    $referenceElsToRemap[] = [$referenceEl, $newPath, $row['type'], $propsData];
                }
            }

            $originalUuid = $row['identifier'];

            // when copying a node, the copy is always a new node. set $isNewNode to true
            $newNodeId = $this->syncNode(null, $newPath, $row['type'], true, [], $propsData);

            if ($references) {
                $referencesToUpdate[$newNodeId] = [
                    'path' => $row['path'],
                    'properties' => $references,
                ];
            }

            $newUuid = $this->nodeIdentifiers[$newPath];
            $uuidMap[$originalUuid] = $newUuid;

            $query = 'INSERT INTO phpcr_binarydata (node_id, property_name, workspace_name, idx, data)'.'   SELECT ?, b.property_name, ?, b.idx, b.data FROM phpcr_binarydata b WHERE b.node_id = ?';

            try {
                $this->getConnection()->executeQuery($query, [$newNodeId, $this->workspaceName, $row['id']]);
            } catch (DBALException $e) {
                throw new RepositoryException(
                    "Unexpected exception while copying node from $srcAbsPath to $dstAbsPath",
                    $e->getCode(),
                    $e
                );
            }
        }

        foreach ($referenceElsToRemap as $data) {
            [$referenceEl, $newPath, $type, $propsData] = $data;
            $referenceEl->nodeValue = $uuidMap[$referenceEl->nodeValue];

            $this->syncNode($this->nodeIdentifiers[$newPath], $newPath, $type, false, [], $propsData);
        }

        $this->syncReferences($referencesToUpdate);
    }

    /**
     * @return string[] First element is the namespace, second the local name
     *
     * @throws NamespaceException
     */
    private function getJcrName(string $path): array
    {
        $name = implode('', array_slice(explode('/', $path), -1, 1));
        $alias = '';

        if (($aliasLength = strpos($name, ':')) !== false) {
            $alias = substr($name, 0, $aliasLength);
            $name = substr($name, $aliasLength + 1);
        }

        $namespaces = $this->getNamespaces();

        if (!isset($namespaces[$alias])) {
            throw new NamespaceException("the namespace $alias was not registered.");
        }

        return [$namespaces[$alias], $name];
    }

    /**
     * Actually write the node into the database.
     *
     * @param bool       $isNewNode new nodes to insert (true) or existing node to update (false)
     * @param Property[] $props
     *
     * @return bool|string
     *
     * @throws ItemExistsException
     * @throws RepositoryException
     * @throws NamespaceException
     * @throws Exception
     */
    private function syncNode(?string $uuid, string $path, string $type, bool $isNewNode, $props = [], $propsData = [])
    {
        // TODO: Not sure if there are always ALL props in $props, should we grab the online data here?
        // TODO: PERFORMANCE Binary data is handled very inefficiently here, UPSERT will really be necessary here as well as lazy handling

        if (!$propsData) {
            $propsData = $this->propsToXML($props);
        }

        if (null === $uuid) {
            $uuid = $this->generateUuid();
        }

        if ($isNewNode) {
            [$namespace, $localName] = $this->getJcrName($path);

            $qb = $this->getConnection()->createQueryBuilder();

            $qb->select(
                ':identifier, :type, :path, :local_name, :namespace, :parent, :workspace_name, :props, :numerical_props, :depth, COALESCE(MAX(n.sort_order), 0) + 1'
            )->from('phpcr_nodes', 'n')->where('n.parent = :parent_a');

            $sql = $qb->getSQL();

            try {
                $insert = 'INSERT INTO phpcr_nodes (identifier, type, path, local_name, namespace, parent, workspace_name, props, numerical_props, depth, sort_order) '.$sql;

                $this->getConnection()->executeQuery(
                    $insert,
                    [
                        'identifier' => $uuid,
                        'type' => $type,
                        'path' => $path,
                        'local_name' => $localName,
                        'namespace' => $namespace,
                        'parent' => PathHelper::getParentPath($path),
                        'workspace_name' => $this->workspaceName,
                        'props' => $propsData['stringDom']->saveXML(),
                        'numerical_props' => $propsData['numericalDom'] ? $propsData['numericalDom']->saveXML() : null,
                        'depth' => PathHelper::getPathDepth($path),
                        'parent_a' => PathHelper::getParentPath($path),
                    ]
                );
            } catch (Exception $e) {
                if ($e instanceof DBALException) {
                    if (false !== strpos($e->getMessage(), 'SQLSTATE[23')) {
                        throw new ItemExistsException('Item '.$path.' already exists in the database');
                    } else {
                        throw new RepositoryException(
                            'Unknown database error while inserting item '.$path.': '.$e->getMessage(),
                            0,
                            $e
                        );
                    }
                } else {
                    throw $e;
                }
            }

            $nodeId = $this->getConnection()->lastInsertId($this->sequenceNodeName);
        } else {
            $nodeId = $this->getSystemIdForNode($path);
            if (!$nodeId) {
                throw new RepositoryException("nodeId for $path not found");
            }

            $this->getConnection()->update(
                'phpcr_nodes',
                [
                    'props' => $propsData['stringDom']->saveXML(),
                    'numerical_props' => $propsData['numericalDom'] ? $propsData['numericalDom']->saveXML() : null,
                ],
                ['id' => $nodeId]
            );
        }

        $this->nodeIdentifiers[$path] = $uuid;

        if (!empty($propsData['binaryData'])) {
            $this->syncBinaryData($nodeId, $propsData['binaryData']);
        }

        $this->referencesToUpdate[$nodeId] = ['path' => $path, 'properties' => $propsData['references']];

        return $nodeId;
    }

    private function syncBinaryData(string $nodeId, array $binaryData): void
    {
        $connection = $this->getConnection();
        foreach ($binaryData as $propertyName => $binaryValues) {
            foreach ($binaryValues as $idx => $data) {
                // TODO verify in which cases we can just update
                $params = [
                    'node_id' => $nodeId,
                    'property_name' => $propertyName,
                    'workspace_name' => $this->workspaceName,
                ];

                $connection->delete('phpcr_binarydata', $params);

                $params['idx'] = $idx;
                $params['data'] = $data;
                $types = [
                    ParameterType::INTEGER,
                    ParameterType::STRING,
                    ParameterType::STRING,
                    ParameterType::INTEGER,
                    ParameterType::LARGE_OBJECT,
                ];

                $connection->insert('phpcr_binarydata', $params, $types);
            }
        }
    }

    /**
     * @throws RepositoryException
     * @throws ReferentialIntegrityException
     */
    private function syncReferences(array $referencesToUpdate): void
    {
        if ($referencesToUpdate) {
            // do not update references that are going to be deleted anyways
            $toUpdate = array_diff(array_keys($referencesToUpdate), array_keys($this->referencesToDelete));

            try {
                foreach ($this->referenceTables as $table) {
                    $query = "DELETE FROM $table WHERE source_id IN (?)";
                    $this->executeChunkedUpdate($query, $toUpdate);
                }
            } catch (DBALException $e) {
                throw new RepositoryException('Unexpected exception while cleaning up after saving', $e->getCode(), $e);
            }

            $updates = [];
            foreach ($toUpdate as $nodeId) {
                $references = $referencesToUpdate[$nodeId];
                foreach ($references['properties'] as $name => $data) {
                    foreach ($data['values'] as $value) {
                        $targetId = $this->getSystemIdForNode($value);
                        if (false === $targetId) {
                            if (PropertyType::REFERENCE === $data['type']) {
                                throw new ReferentialIntegrityException(
                                    sprintf(
                                        'Trying to store reference to non-existant node with path "%s" in node "%s" "%s"',
                                        $value,
                                        $references['path'],
                                        $name
                                    )
                                );
                            }

                            continue;
                        }

                        $key = $targetId.'-'.$nodeId.'-'.$name;
                        // it is valid to have multiple references to the same node in a multivalue
                        // but it is not desired to store duplicates in the database
                        $updates[$key] = [
                            'type' => $data['type'],
                            'data' => [
                                'source_id' => $nodeId,
                                'source_property_name' => $name,
                                'target_id' => $targetId,
                            ],
                        ];
                    }
                }
            }

            foreach ($updates as $update) {
                $this->getConnection()->insert($this->referenceTables[$update['type']], $update['data']);
            }
        }

        // TODO on RDBMS that support deferred FKs we could skip this step
        if ($this->referencesToDelete) {
            $params = array_keys($this->referencesToDelete);

            // remove all PropertyType::REFERENCE with a source_id on a deleted node
            try {
                $query = 'DELETE FROM phpcr_nodes_references WHERE source_id IN (?)';
                $this->executeChunkedUpdate($query, $params);
            } catch (DBALException $e) {
                throw new RepositoryException(
                    'Unexpected exception while cleaning up deleted nodes',
                    $e->getCode(),
                    $e
                );
            }

            // ensure that there are no PropertyType::REFERENCE pointing to nodes that will be deleted
            // Note: due to the outer join we cannot filter on workspace_name, but this is ok
            // since within a transaction there can never be missing referenced nodes within the current workspace
            // make sure the target node is not in the list of nodes being deleted, to allow deletion in same request
            $query = 'SELECT DISTINCT r.target_id
            FROM phpcr_nodes_references r
                LEFT OUTER JOIN phpcr_nodes n ON r.target_id = n.id
            WHERE r.target_id IN (?)';

            if ($this->getConnection()->getDatabasePlatform() instanceof SqlitePlatform) {
                $missingTargets = [];
                foreach (array_chunk($params, self::SQLITE_MAXIMUM_IN_PARAM_COUNT) as $chunk) {
                    $stmt = $this->getConnection()->executeQuery($query, [$chunk], [Connection::PARAM_INT_ARRAY]);
                    $missingTargets = array_merge($missingTargets, array_column($stmt->fetchAllNumeric(), 0));
                }
            } else {
                $stmt = $this->getConnection()->executeQuery($query, [$params], [Connection::PARAM_INT_ARRAY]);
                $missingTargets = array_column($stmt->fetchAllNumeric(), 0);
            }
            if ($missingTargets) {
                $paths = [];
                foreach ($missingTargets as $id) {
                    if (isset($this->referencesToDelete[$id])) {
                        $paths[] = $this->referencesToDelete[$id];
                    }
                }

                throw new ReferentialIntegrityException(
                    "Cannot delete '".implode("', '", $paths)."': A reference points to this node or a subnode"
                );
            }

            // clean up all references
            try {
                foreach ($this->referenceTables as $table) {
                    $query = "DELETE FROM $table WHERE target_id IN (?)";
                    $this->executeChunkedUpdate($query, $params);
                }
            } catch (DBALException $e) {
                throw new RepositoryException(
                    'Unexpected exception while cleaning up deleted nodes',
                    $e->getCode(),
                    $e
                );
            }
        }
    }

    /**
     * Convert the node XML to stdClass node representation containing all properties.
     *
     * @throws InvalidArgumentException
     */
    private function xmlToProps(string $xml): stdClass
    {
        $xmlParser = new XmlToPropsParser(
            $xml,
            $this->valueConverter
        );

        return $xmlParser->parse();
    }

    /**
     * Convert the node XML to stdClass node representation containing only the given properties.
     *
     * @param string[] $propertyNames
     *
     * @throws InvalidArgumentException
     */
    private function xmlToColumns(string $xml, array $propertyNames): stdClass
    {
        $xmlParser = new XmlToPropsParser(
            $xml,
            $this->valueConverter,
            $propertyNames
        );

        return $xmlParser->parse();
    }

    /**
     * Seperate properties array into an xml and binary data.
     *
     * @param Property[] $properties
     *
     * @return array (
     *               'stringDom' => $stringDom,
     *               'numericalDom' => $numericalDom',
     *               'binaryData' => streams,
     *               'references' => ['type' => INT, 'values' => [UUIDs])
     *               )
     *
     * @throws RepositoryException
     */
    private function propsToXML($properties): array
    {
        $namespaces = [
            NS::PREFIX_MIX => NS::NAMESPACE_MIX,
            NS::PREFIX_NT => NS::NAMESPACE_NT,
            'xs' => 'http://www.w3.org/2001/XMLSchema',
            NS::PREFIX_JCR => NS::NAMESPACE_JCR,
            NS::PREFIX_SV => NS::NAMESPACE_SV,
            'rep' => 'internal',
        ];

        $doms = [
            'stringDom' => [],
            'numericalDom' => [],
        ];

        $binaryData = $references = [];

        foreach ($properties as $property) {
            $targetDoms = ['stringDom'];

            switch ($property->getType()) {
                case PropertyType::WEAKREFERENCE:
                case PropertyType::REFERENCE:
                    $references[$property->getName()] = [
                        'type' => $property->getType(),
                        'values' => $property->isMultiple() ? $property->getString() : [$property->getString()],
                    ];
                    // no break
                case PropertyType::NAME:
                case PropertyType::URI:
                case PropertyType::PATH:
                case PropertyType::STRING:
                    $values = $property->getString();
                    break;
                case PropertyType::DECIMAL:
                    $values = $property->getDecimal();
                    $targetDoms[] = 'numericalDom';
                    break;
                case PropertyType::BOOLEAN:
                    $values = array_map('intval', (array) $property->getBoolean());
                    break;
                case PropertyType::LONG:
                    $values = $property->getLong();
                    $targetDoms[] = 'numericalDom';
                    break;
                case PropertyType::BINARY:
                    if ($property->isNew() || $property->isModified()) {
                        $values = [];
                        foreach ((array) $property->getValueForStorage() as $stream) {
                            if (null === $stream) {
                                $binary = '';
                            } else {
                                $binary = stream_get_contents($stream);
                                fclose($stream);
                            }
                            $binaryData[$property->getName()][] = $binary;
                            $length = strlen($binary);
                            $values[] = $length;
                        }
                    } else {
                        $values = $property->getLength();
                        if (!$property->isMultiple() && empty($values)) {
                            $values = [0];
                        }
                    }
                    break;
                case PropertyType::DATE:
                    $values = $property->getDate();
                    if ($values instanceof DateTime) {
                        $values = [$values];
                    }
                    foreach ((array) $values as $key => $date) {
                        if ($date instanceof DateTime) {
                            // do not modify the instance which is associated with the node.
                            $date = clone $date;

                            // normalize to UTC for storage.
                            $date->setTimezone(new DateTimeZone('UTC'));
                        }
                        $values[$key] = $date;
                    }
                    $values = $this->valueConverter->convertType($values, PropertyType::STRING);
                    break;
                case PropertyType::DOUBLE:
                    $values = $property->getDouble();
                    $targetDoms[] = 'numericalDom';
                    break;
                default:
                    throw new RepositoryException('unknown type '.$property->getType());
            }

            foreach ($targetDoms as $targetDom) {
                $doms[$targetDom][] = [
                    'name' => $property->getName(),
                    'type' => PropertyType::nameFromValue($property->getType()),
                    'multiple' => $property->isMultiple(),
                    'lengths' => (array) $property->getLength(),
                    'values' => $values,
                ];
            }
        }

        $ret = [
            'stringDom' => null,
            'numericalDom' => null,
            'binaryData' => $binaryData,
            'references' => $references,
        ];

        foreach ($doms as $targetDom => $properties) {
            $dom = new DOMDocument('1.0', 'UTF-8');
            $rootNode = $dom->createElement('sv:node');
            foreach ($namespaces as $namespace => $uri) {
                $rootNode->setAttribute('xmlns:'.$namespace, $uri);
            }
            $dom->appendChild($rootNode);

            foreach ($properties as $property) {
                /* @var $property Property */
                $propertyNode = $dom->createElement('sv:property');
                $propertyNode->setAttribute('sv:name', $property['name']);
                $propertyNode->setAttribute('sv:type', $property['type']);
                $propertyNode->setAttribute('sv:multi-valued', $property['multiple'] ? '1' : '0');
                $lengths = (array) $property['lengths'];
                foreach ((array) $property['values'] as $key => $value) {
                    $element = $propertyNode->appendChild($dom->createElement('sv:value'));
                    $element->appendChild($dom->createTextNode($value));
                    if (isset($lengths[$key])) {
                        $lengthAttribute = $dom->createAttribute('length');
                        $lengthAttribute->value = $lengths[$key];
                        $element->appendChild($lengthAttribute);
                    }
                }

                $rootNode->appendChild($propertyNode);
            }

            if (count($properties)) {
                $ret[$targetDom] = $dom;
            }
        }

        return $ret;
    }

    /**
     * {@inheritDoc}
     *
     * @throws DBALException
     */
    public function getAccessibleWorkspaceNames(): array
    {
        $query = 'SELECT DISTINCT name FROM phpcr_workspaces';
        $stmt = $this->getConnection()->executeQuery($query);

        return array_column($stmt->fetchAllNumeric(), 0);
    }

    /**
     * {@inheritDoc}
     *
     * @throws RepositoryException
     * @throws DBALException
     */
    public function getNode($path)
    {
        $this->assertLoggedIn();
        PathHelper::assertValidAbsolutePath($path, false, true, $this->getNamespacePrefixes());

        $values['path'] = $path;
        $values['pathd'] = rtrim($path, '/').'/%';
        $values['workspace'] = $this->workspaceName;

        if ($this->fetchDepth > 0) {
            $values['fetchDepth'] = $this->fetchDepth;
            $query = '
              SELECT * FROM phpcr_nodes
              WHERE (path LIKE :pathd OR path = :path)
                AND workspace_name = :workspace
                AND depth <= ((SELECT depth FROM phpcr_nodes WHERE path = :path AND workspace_name = :workspace) + :fetchDepth)
              ORDER BY depth, sort_order ASC';
        } else {
            $query = '
              SELECT * FROM phpcr_nodes
              WHERE path = :path
                AND workspace_name = :workspace
              ORDER BY depth, sort_order ASC';
        }

        $stmt = $this->getConnection()->executeQuery($query, $values);
        $rows = $stmt->fetchAllAssociative();
        if (empty($rows)) {
            throw new ItemNotFoundException("Item $path not found in workspace ".$this->workspaceName);
        }

        $nestedNodes = $this->getNodesData($rows);
        $node = array_shift($nestedNodes);
        foreach ($nestedNodes as $nestedPath => $nested) {
            $relativePath = PathHelper::relativizePath($nestedPath, $path);
            $this->nestNode($node, $nested, explode('/', $relativePath));
        }

        return $node;
    }

    /**
     * Attach a node at a subpath under the ancestor node.
     *
     * @param string[] $nodeNames Breadcrumb of child nodes from parentNode to the node itself
     */
    private function nestNode(stdClass $ancestor, stdClass $node, array $nodeNames): void
    {
        while ($name = array_shift($nodeNames)) {
            if (empty($nodeNames)) {
                $ancestor->{$name} = $node;

                return;
            }
            $ancestor = $ancestor->{$name};
        }
    }

    /**
     * Convert a node result row to the stdClass representing all raw data.
     *
     * @return stdClass raw node data
     */
    private function getNodeData(array $row): stdClass
    {
        $data = $this->getNodesData([$row]);

        return array_shift($data);
    }

    /**
     * Build the raw data for a list of database result rows, fetching the
     * additional information in one single query.
     *
     * @return stdClass[]
     *
     * @throws NoSuchNodeTypeException
     * @throws RepositoryException
     */
    private function getNodesData(array $rows): array
    {
        $data = [];
        $paths = [];

        foreach ($rows as $row) {
            $this->nodeIdentifiers[$row['path']] = $row['identifier'];
            $data[$row['path']] = $this->xmlToProps($row['props']);
            $data[$row['path']]->{'jcr:primaryType'} = $row['type'];
            $paths[] = $row['path'];
        }

        $query = 'SELECT path, parent FROM phpcr_nodes WHERE parent IN (?) AND workspace_name = ? ORDER BY sort_order ASC';
        if ($this->getConnection()->getDatabasePlatform() instanceof SqlitePlatform) {
            $childrenRows = [];
            foreach (array_chunk($paths, self::SQLITE_MAXIMUM_IN_PARAM_COUNT) as $chunk) {
                $childrenRows += $this->getConnection()->fetchAllAssociative(
                    $query,
                    [$chunk, $this->workspaceName],
                    [Connection::PARAM_STR_ARRAY, null]
                );
            }
        } else {
            $childrenRows = $this->getConnection()->fetchAllAssociative(
                $query,
                [$paths, $this->workspaceName],
                [Connection::PARAM_STR_ARRAY, null]
            );
        }

        foreach ($childrenRows as $child) {
            $childName = explode('/', $child['path']);
            $childName = end($childName);
            if (!isset($data[$child['parent']]->{$childName})) {
                $data[$child['parent']]->{$childName} = new stdClass();
            }
        }

        foreach (array_keys($data) as $path) {
            // If the node is referenceable, return jcr:uuid.
            if (isset($data[$path]->{'jcr:mixinTypes'})) {
                foreach ((array) $data[$path]->{'jcr:mixinTypes'} as $mixin) {
                    if ($this->nodeTypeManager->getNodeType($mixin)->isNodeType('mix:referenceable')) {
                        $data[$path]->{'jcr:uuid'} = $this->nodeIdentifiers[$path];
                        break;
                    }
                }
            }
        }

        return $data;
    }

    /**
     * {@inheritDoc}
     *
     * @throws RepositoryException
     */
    public function getNodes($paths)
    {
        $this->assertLoggedIn();

        if (empty($paths)) {
            return [];
        }

        foreach ($paths as $path) {
            PathHelper::assertValidAbsolutePath($path, false, true, $this->getNamespacePrefixes());
        }

        $params['workspace'] = $this->workspaceName;

        if ($this->fetchDepth > 0) {
            $params['fetchDepth'] = $this->fetchDepth;

            $query = '
              SELECT path AS arraykey, id, path, parent, local_name, namespace, workspace_name, identifier, type, props, depth, sort_order
              FROM phpcr_nodes
              WHERE workspace_name = :workspace
                AND (';

            $i = 0;
            foreach ($paths as $path) {
                $params['path'.$i] = $path;
                $params['pathd'.$i] = rtrim($path, '/').'/%';
                $subquery = 'SELECT depth FROM phpcr_nodes WHERE path = :path'.$i.' AND workspace_name = :workspace';
                $query .= '(path LIKE :pathd'.$i.' OR path = :path'.$i.') AND depth <= (('.$subquery.') + :fetchDepth) OR ';
                ++$i;
            }
        } else {
            $query = 'SELECT path AS arraykey, id, path, parent, local_name, namespace, workspace_name, identifier, type, props, depth, sort_order
                FROM phpcr_nodes WHERE workspace_name = :workspace AND (';

            $i = 0;
            foreach ($paths as $path) {
                $params['path'.$i] = $path;
                $query .= 'path = :path'.$i.' OR ';
                ++$i;
            }
        }

        $query = rtrim($query, 'OR ');
        $query .= ') ORDER BY sort_order ASC';

        $stmt = $this->getConnection()->executeQuery($query, $params);

        // emulate old $stmt->fetchAll(PDO::FETCH_UNIQUE | PDO::FETCH_GROUP)
        $all = [];
        while ($row = $stmt->fetchAssociative()) {
            $index = array_shift($row);
            $all[$index] = $row;
        }

        $nodes = [];
        if ($all) {
            $nodes = $this->getNodesData($all);
        }

        return $nodes;
    }

    /**
     * Determine if a path exists already.
     *
     * @param string      $path          Path of the node
     * @param string|null $workspaceName To overwrite the current workspace
     */
    private function pathExists(string $path, string $workspaceName = null): bool
    {
        return (bool) $this->getSystemIdForNode($path, $workspaceName);
    }

    /**
     * Get the database primary key for node.
     *
     * @param string      $identifier    Path of the identifier
     * @param string|null $workspaceName To overwrite the current workspace
     *
     * @return bool|string The database id
     */
    private function getSystemIdForNode(string $identifier, string $workspaceName = null)
    {
        if (null === $workspaceName) {
            $workspaceName = $this->workspaceName;
        }

        if (UUIDHelper::isUUID($identifier)) {
            $query = 'SELECT id FROM phpcr_nodes WHERE identifier = ? AND workspace_name = ?';
        } else {
            $platform = $this->getConnection()->getDatabasePlatform();
            if ($platform instanceof MySQLPlatform) {
                $query = 'SELECT id FROM phpcr_nodes WHERE path COLLATE '.$this->getCaseSensitiveEncoding().' = ? AND workspace_name = ?';
            } else {
                $query = 'SELECT id FROM phpcr_nodes WHERE path = ? AND workspace_name = ?';
            }
        }

        $nodeId = $this->getConnection()->fetchOne($query, [$identifier, $workspaceName]);

        return $nodeId ?: false;
    }

    /**
     * {@inheritDoc}
     */
    public function getNodeByIdentifier($uuid)
    {
        $this->assertLoggedIn();

        $query = 'SELECT * FROM phpcr_nodes WHERE identifier = ? AND workspace_name = ?';
        $row = $this->getConnection()->fetchAssociative($query, [$uuid, $this->workspaceName]);
        if (!$row) {
            throw new ItemNotFoundException("Item $uuid not found in workspace ".$this->workspaceName);
        }

        $path = $row['path'];
        $data = $this->getNodeData($row);
        $data->{':jcr:path'} = $path;

        return $data;
    }

    /**
     * {@inheritDoc}
     */
    public function getNodesByIdentifier($identifiers): array
    {
        $this->assertLoggedIn();

        if (empty($identifiers)) {
            return [];
        }

        $query = 'SELECT id, path, parent, local_name, namespace, workspace_name, identifier, type, props, depth, sort_order
            FROM phpcr_nodes WHERE workspace_name = ? AND identifier IN (?)';
        if ($this->getConnection()->getDatabasePlatform() instanceof SqlitePlatform) {
            $all = [];
            foreach (array_chunk($identifiers, self::SQLITE_MAXIMUM_IN_PARAM_COUNT) as $chunk) {
                $all += $this->getConnection()->fetchAllAssociative(
                    $query,
                    [$this->workspaceName, $chunk],
                    [ParameterType::STRING, Connection::PARAM_STR_ARRAY]
                );
            }
        } else {
            $all = $this->getConnection()->fetchAllAssociative(
                $query,
                [$this->workspaceName, $identifiers],
                [ParameterType::STRING, Connection::PARAM_STR_ARRAY]
            );
        }

        $nodes = [];
        if ($all) {
            $nodesData = $this->getNodesData($all);
            // ensure that the nodes are returned in the order if how the identifiers were passed in
            $pathByUuid = [];
            foreach ($nodesData as $path => $node) {
                $pathByUuid[$node->{'jcr:uuid'}] = $path;
            }
            foreach ($identifiers as $identifier) {
                if (isset($pathByUuid[$identifier])) {
                    $nodes[$pathByUuid[$identifier]] = $nodesData[$pathByUuid[$identifier]];
                }
            }
        }

        return $nodes;
    }

    /**
     * {@inheritDoc}
     *
     * @throws NotImplementedException
     * @throws ItemNotFoundException
     */
    public function getNodePathForIdentifier($uuid, $workspace = null): string
    {
        if (null !== $workspace) {
            throw new NotImplementedException('Specifying the workspace is not yet supported.');
        }

        $this->assertLoggedIn();

        $query = 'SELECT path FROM phpcr_nodes WHERE identifier = ? AND workspace_name = ?';
        $path = $this->getConnection()->fetchColumn($query, [$uuid, $this->workspaceName]);
        if (!$path) {
            throw new ItemNotFoundException('no item found with uuid '.$uuid);
        }

        return $path;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteNodes(array $operations): bool
    {
        $this->assertLoggedIn();

        foreach ($operations as $op) {
            $this->deleteNode($op->srcPath);
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function deleteNodeImmediately($path): bool
    {
        $this->prepareSave();
        $this->deleteNode($path);
        $this->finishSave();

        return true;
    }

    /**
     * TODO instead of calling the deletes separately, we should batch the delete query
     * but careful with the caching!
     *
     * @param string $path node path to delete
     *
     * @throws ConstraintViolationException
     * @throws ItemNotFoundException
     * @throws RepositoryException
     */
    private function deleteNode($path): void
    {
        if ('/' === $path) {
            throw new ConstraintViolationException('You can not delete the root node of a repository');
        }

        if (!$this->pathExists($path)) {
            throw new ItemNotFoundException('No node found at '.$path);
        }

        $params = [$path, $path.'/%', $this->workspaceName];

        // TODO on RDBMS that support deferred FKs we could skip this step
        $query = 'SELECT id, path FROM phpcr_nodes WHERE (path = ? OR path LIKE ?) AND workspace_name = ?';
        $stmt = $this->getConnection()->executeQuery($query, $params);
        $this->referencesToDelete += array_column($stmt->fetchAllNumeric(), 1, 0);

        try {
            $query = 'DELETE FROM phpcr_nodes WHERE (path = ? OR path LIKE ?) AND workspace_name = ?';
            $this->getConnection()->executeQuery($query, $params);
            $this->cleanIdentifierCache($path);
        } catch (DBALException $e) {
            throw new RepositoryException('Unexpected exception while deleting node '.$path, $e->getCode(), $e);
        }
    }

    /**
     * Clean all identifiers under path $root.
     *
     * @param string $root Path to the root node to be cleared
     */
    private function cleanIdentifierCache(string $root): void
    {
        unset($this->nodeIdentifiers[$root]);
        foreach (array_keys($this->nodeIdentifiers) as $path) {
            if (0 === strpos($path, "$root/")) {
                unset($this->nodeIdentifiers[$path]);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public function deleteProperties(array $operations): bool
    {
        $this->assertLoggedIn();

        foreach ($operations as $op) {
            $this->deleteProperty($op->srcPath);
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function deletePropertyImmediately($path): bool
    {
        $this->prepareSave();
        $this->deleteProperty($path);
        $this->finishSave();

        return true;
    }

    /**
     * @throws ItemNotFoundException
     * @throws RepositoryException
     */
    private function deleteProperty(string $path): void
    {
        $this->assertLoggedIn();

        $nodePath = PathHelper::getParentPath($path);
        $nodeId = $this->getSystemIdForNode($nodePath);
        if (!$nodeId) {
            // no we really don't know that path
            throw new ItemNotFoundException('No item found at '.$path);
        }

        $query = 'SELECT props FROM phpcr_nodes WHERE id = ?';
        $xml = $this->getConnection()->fetchOne($query, [$nodeId]);

        $dom = new DOMDocument('1.0', 'UTF-8');
        $dom->loadXML($xml);

        $found = false;
        $propertyName = PathHelper::getNodeName($path);
        foreach ($dom->getElementsByTagNameNS('http://www.jcp.org/jcr/sv/1.0', 'property') as $propertyNode) {
            if ($propertyName == $propertyNode->getAttribute('sv:name')) {
                $found = true;
                // would be nice to have the property object to ask for type
                // but its in state deleted, would mean lots of refactoring
                if ($propertyNode->hasAttribute('sv:type')) {
                    $type = strtolower($propertyNode->getAttribute('sv:type'));
                    if (in_array($type, ['reference', 'weakreference'])) {
                        $table = $this->referenceTables['reference' === $type ? PropertyType::REFERENCE : PropertyType::WEAKREFERENCE];
                        try {
                            $query = "DELETE FROM $table WHERE source_id = ? AND source_property_name = ?";
                            $this->getConnection()->executeQuery($query, [$nodeId, $propertyName]);
                        } catch (DBALException $e) {
                            throw new RepositoryException(
                                'Unexpected exception while cleaning up deleted nodes',
                                $e->getCode(),
                                $e
                            );
                        }
                    }
                }
                $propertyNode->parentNode->removeChild($propertyNode);
                break;
            }
        }

        if (!$found) {
            throw new ItemNotFoundException("Node $nodePath has no property $propertyName");
        }

        $xml = $dom->saveXML();

        $query = 'UPDATE phpcr_nodes SET props = ? WHERE id = ?';
        $params = [$xml, $nodeId];

        try {
            $this->getConnection()->executeQuery($query, $params);
        } catch (DBALException $e) {
            throw new RepositoryException("Unexpected exception while updating properties of $path", $e->getCode(), $e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function moveNodes(array $operations): bool
    {
        /** @var $op MoveNodeOperation */
        foreach ($operations as $op) {
            $this->moveNode($op->srcPath, $op->dstPath);
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function moveNodeImmediately($srcAbsPath, $dstAbspath): bool
    {
        $this->prepareSave();
        $this->moveNode($srcAbsPath, $dstAbspath);
        $this->finishSave();

        return true;
    }

    /**
     * Execute moving a single node.
     *
     * @throws PathNotFoundException
     * @throws ItemExistsException
     * @throws RepositoryException
     */
    private function moveNode($srcAbsPath, $dstAbsPath): void
    {
        $this->assertLoggedIn();
        PathHelper::assertValidAbsolutePath($srcAbsPath, false, true, $this->getNamespacePrefixes());
        PathHelper::assertValidAbsolutePath($dstAbsPath, true, true, $this->getNamespacePrefixes());

        if (!$this->pathExists($srcAbsPath)) {
            throw new PathNotFoundException("Source path '$srcAbsPath' not found");
        }

        if ($this->getSystemIdForNode($dstAbsPath)) {
            throw new ItemExistsException(
                "Cannot move '$srcAbsPath' to '$dstAbsPath' because destination node already exists."
            );
        }

        if (!$this->getSystemIdForNode(PathHelper::getParentPath($dstAbsPath))) {
            throw new PathNotFoundException("Parent of the destination path '".$dstAbsPath."' has to exist.");
        }

        $query = 'SELECT path, id FROM phpcr_nodes WHERE path LIKE ? OR path = ? AND workspace_name = ? '.$this->getConnection(
            )->getDatabasePlatform()->getForUpdateSQL();
        $stmt = $this->getConnection()->executeQuery($query, [$srcAbsPath.'/%', $srcAbsPath, $this->workspaceName]);

        /*
         * TODO: https://github.com/jackalope/jackalope-doctrine-dbal/pull/26/files#L0R1057
         * the other thing i wonder: can't you do the replacement inside sql instead of loading and then storing
         * the node? this will be extremely slow for a large set of nodes. i think you should use query builder here
         * rather than raw sql, to make it work on a maximum of platforms.
         *
         * can you try to do this please? if we don't figure out how to do it, at least fix the where criteria, and
         * we can ask the doctrine community how to do the substring operation.
         * http://stackoverflow.com/questions/8619421/correct-syntax-for-doctrine2s-query-builder-substring-helper-method
         */

        $query = 'UPDATE phpcr_nodes SET ';
        $updatePathCase = 'path = CASE ';
        $updateParentCase = 'parent = CASE ';
        $updateLocalNameCase = 'local_name = CASE ';
        $updateSortOrderCase = 'sort_order = CASE ';
        $updateDepthCase = 'depth = CASE ';

        // TODO: Find a better way to do this
        // Calculate CAST type for CASE statement
        switch ($this->getConnection()->getDatabasePlatform()->getName()) {
            case 'pgsql':
                $intType = 'integer';
                break;
            case 'mysql':
                $intType = 'unsigned';
                break;
            default:
                $intType = 'integer';
        }

        $i = 0;
        $values = $ids = [];
        $srcAbsPathPattern = '/^'.preg_quote($srcAbsPath, '/').'/';
        while ($row = $stmt->fetchAssociative()) {
            $values['id'.$i] = $row['id'];
            $values['path'.$i] = preg_replace($srcAbsPathPattern, $dstAbsPath, $row['path'], 1);
            $values['parent'.$i] = PathHelper::getParentPath($values['path'.$i]);
            $values['depth'.$i] = PathHelper::getPathDepth($values['path'.$i]);

            $updatePathCase .= "WHEN id = :id$i THEN :path$i ";
            $updateParentCase .= "WHEN id = :id$i THEN :parent$i ";
            $updateDepthCase .= "WHEN id = :id$i THEN CAST(:depth$i AS $intType) ";

            if ($srcAbsPath === $row['path']) {
                [, $localName] = $this->getJcrName($values['path'.$i]);
                $values['localname'.$i] = $localName;

                $updateLocalNameCase .= "WHEN id = :id$i THEN :localname$i ";
                $updateSortOrderCase .= "WHEN id = :id$i THEN (SELECT * FROM ( SELECT MAX(x.sort_order) + 1 FROM phpcr_nodes x WHERE x.parent = :parent$i) y) ";
            }

            $ids[] = $row['id'];

            ++$i;
        }

        if (!$i) {
            return;
        }

        $ids = implode(',', $ids);

        $updateLocalNameCase .= 'ELSE local_name END, ';
        $updateSortOrderCase .= 'ELSE sort_order END ';

        $query .= $updatePathCase.'END, '.$updateParentCase.'END, '.$updateDepthCase.'END, '.$updateLocalNameCase.$updateSortOrderCase;
        $query .= "WHERE id IN ($ids)";

        try {
            $this->getConnection()->executeStatement($query, $values);
        } catch (DBALException $e) {
            throw new RepositoryException(
                "Unexpected exception while moving node from $srcAbsPath to $dstAbsPath",
                $e->getCode(),
                $e
            );
        }

        $this->cleanIdentifierCache($srcAbsPath);
    }

    /**
     * {@inheritDoc}
     *
     * @throws RepositoryException
     */
    public function reorderChildren(Node $node): bool
    {
        $this->assertLoggedIn();

        $values['absPath'] = $node->getPath();
        $sql = "UPDATE phpcr_nodes SET sort_order = CASE CONCAT(
          namespace,
          (CASE namespace WHEN '' THEN '' ELSE ':' END),
          local_name
        )";

        $i = 0;

        foreach ($node->getNodeNames() as $name) {
            $values['name'.$i] = implode(':', array_filter($this->getJcrName($name)));
            $values['order'.$i] = $i; // use our counter to avoid gaps
            $sql .= " WHEN :name$i THEN :order$i";
            ++$i;
        }

        $sql .= ' ELSE sort_order END WHERE parent = :absPath';

        try {
            $this->getConnection()->executeStatement($sql, $values);
        } catch (DBALException $e) {
            throw new RepositoryException('Unexpected exception while reordering nodes', $e->getCode(), $e);
        }

        return true;
    }

    /**
     * Return the node processor for processing nodes according to their node types.
     */
    private function getNodeProcessor(): NodeProcessor
    {
        if (isset($this->nodeProcessor)) {
            return $this->nodeProcessor;
        }

        $this->nodeProcessor = new NodeProcessor(
            $this->credentials->getUserID(),
            $this->getNamespacesObject(),
            $this->getAutoLastModified()
        );

        return $this->nodeProcessor;
    }

    /**
     * {@inheritDoc}
     */
    public function storeNodes(array $operations): void
    {
        $this->assertLoggedIn();

        $additionalAddOperations = [];

        foreach ($operations as $operation) {
            if ($operation->node->isDeleted()) {
                $properties = $operation->node->getPropertiesForStoreDeletedNode();
            } else {
                $additionalAddOperations = array_merge(
                    $additionalAddOperations,
                    $this->getNodeProcessor()->process($operation->node)
                );
                $properties = $operation->node->getProperties();
            }
            $this->storeNode($operation->srcPath, $properties);
        }

        if (!empty($additionalAddOperations)) {
            $this->storeNodes($additionalAddOperations);
        }
    }

    /**
     * Make sure we have a uuid and a primaryType, then sync data into the database.
     *
     * @param Property[] $properties
     *
     * @throws RepositoryException if not logged in
     */
    private function storeNode(string $path, $properties): bool
    {
        $nodeIdentifier = $this->getIdentifier($path, $properties);
        $type = isset($properties['jcr:primaryType']) ? $properties['jcr:primaryType']->getValue() : 'nt:unstructured';

        $this->syncNode($nodeIdentifier, $path, $type, true, $properties);

        return true;
    }

    /**
     * Determine a UUID for the node at this path with these properties.
     *
     * @param Property[] $properties
     *
     * @return string a unique id
     */
    private function getIdentifier(string $path, $properties): string
    {
        if (isset($this->nodeIdentifiers[$path])) {
            return $this->nodeIdentifiers[$path];
        }

        if (isset($properties['jcr:uuid'])) {
            return $properties['jcr:uuid']->getValue();
        }

        // we always generate a uuid, even for non-referenceable nodes that have no automatic uuid
        return $this->generateUuid();
    }

    /**
     * {@inheritDoc}
     */
    public function getNodeTypes($nodeTypes = []): array
    {
        $standardTypes = StandardNodeTypes::getNodeTypeData();

        $userTypes = $this->fetchUserNodeTypes();

        if ($nodeTypes) {
            $nodeTypes = array_flip($nodeTypes);

            return array_values(
                array_intersect_key($standardTypes, $nodeTypes) + array_intersect_key($userTypes, $nodeTypes)
            );
        }

        return array_values($standardTypes + $userTypes);
    }

    /**
     * Fetch a user-defined node-type definition.
     */
    protected function fetchUserNodeTypes(): array
    {
        $result = [];

        $query = '
SELECT
phpcr_type_nodes.name AS node_name, phpcr_type_nodes.is_abstract AS node_abstract,
phpcr_type_nodes.is_mixin AS node_mixin, phpcr_type_nodes.queryable AS node_queryable,
phpcr_type_nodes.orderable_child_nodes AS node_has_orderable_child_nodes,
phpcr_type_nodes.primary_item AS node_primary_item_name, phpcr_type_nodes.supertypes AS declared_super_type_names,
phpcr_type_props.name AS property_name, phpcr_type_props.auto_created AS property_auto_created,
phpcr_type_props.mandatory AS property_mandatory, phpcr_type_props.protected AS property_protected,
phpcr_type_props.on_parent_version AS property_on_parent_version,
phpcr_type_props.required_type AS property_required_type, phpcr_type_props.multiple AS property_multiple,
phpcr_type_props.fulltext_searchable AS property_fulltext_searchable,
phpcr_type_props.query_orderable AS property_query_orderable, phpcr_type_props.default_value as property_default_value,
phpcr_type_childs.name AS child_name, phpcr_type_childs.auto_created AS child_auto_created,
phpcr_type_childs.mandatory AS child_mandatory, phpcr_type_childs.protected AS child_protected,
phpcr_type_childs.on_parent_version AS child_on_parent_version, phpcr_type_childs.default_type AS child_default_type,
phpcr_type_childs.primary_types AS child_primary_types
FROM
phpcr_type_nodes
LEFT JOIN
phpcr_type_props ON phpcr_type_nodes.node_type_id = phpcr_type_props.node_type_id
LEFT JOIN
phpcr_type_childs ON phpcr_type_nodes.node_type_id = phpcr_type_childs.node_type_id
';

        if (!isset($this->dbalVersion)) {
            $this->determineDbalVersion();
        }
        $statement = $this->getConnection()->prepare($query);

        $stmtResult = self::DBAL3 === $this->dbalVersion ? $statement->executeQuery() : $statement->execute();
        $stmtResult = is_bool($stmtResult) ? $statement : $stmtResult;

        while ($row = $stmtResult->fetchAssociative()) {
            $nodeName = $row['node_name'];

            if (!isset($result[$nodeName])) {
                $result[$nodeName] = [
                    'name' => $nodeName,
                    'isAbstract' => (bool) $row['node_abstract'],
                    'isMixin' => (bool) $row['node_mixin'],
                    'isQueryable' => (bool) $row['node_queryable'],
                    'hasOrderableChildNodes' => (bool) $row['node_has_orderable_child_nodes'],
                    'primaryItemName' => $row['node_primary_item_name'],
                    'declaredSuperTypeNames' => array_filter(explode(' ', $row['declared_super_type_names'])),
                    'declaredPropertyDefinitions' => [],
                    'declaredNodeDefinitions' => [],
                ];
            }

            if (($propertyName = $row['property_name']) !== null) {
                $result[$nodeName]['declaredPropertyDefinitions'][] = [
                    'declaringNodeType' => $nodeName,
                    'name' => $propertyName,
                    'isAutoCreated' => (bool) $row['property_auto_created'],
                    'isMandatory' => (bool) $row['property_mandatory'],
                    'isProtected' => (bool) $row['property_protected'],
                    'onParentVersion' => (bool) $row['property_on_parent_version'],
                    'requiredType' => (int) $row['property_required_type'],
                    'multiple' => (bool) $row['property_multiple'],
                    'isFulltextSearchable' => (bool) $row['property_fulltext_searchable'],
                    'isQueryOrderable' => (bool) $row['property_query_orderable'],
                    'queryOperators' => [
                        QOM::JCR_OPERATOR_EQUAL_TO,
                        QOM::JCR_OPERATOR_NOT_EQUAL_TO,
                        QOM::JCR_OPERATOR_GREATER_THAN,
                        QOM::JCR_OPERATOR_GREATER_THAN_OR_EQUAL_TO,
                        QOM::JCR_OPERATOR_LESS_THAN,
                        QOM::JCR_OPERATOR_LESS_THAN_OR_EQUAL_TO,
                        QOM::JCR_OPERATOR_LIKE,
                    ],
                    'defaultValues' => [$row['property_default_value']],
                ];
            }

            if (($childName = $row['child_name']) !== null) {
                $result[$nodeName]['declaredNodeDefinitions'][] = [
                    'declaringNodeType' => $nodeName,
                    'name' => $childName,
                    'isAutoCreated' => (bool) $row['child_auto_created'],
                    'isMandatory' => (bool) $row['child_mandatory'],
                    'isProtected' => (bool) $row['child_protected'],
                    'onParentVersion' => (bool) $row['child_on_parent_version'],
                    'allowsSameNameSiblings' => false,
                    'defaultPrimaryTypeName' => $row['child_default_type'],
                    'requiredPrimaryTypeNames' => array_filter(explode(' ', $row['child_primary_types'])),
                ];
            }
        }

        return $result;
    }

    /**
     * {@inheritDoc}
     */
    public function registerNodeTypes($types, $allowUpdate): bool
    {
        $builtinTypes = StandardNodeTypes::getNodeTypeData();

        /* @var $type NodeTypeDefinition */
        foreach ($types as $type) {
            if (isset($builtinTypes[$type->getName()])) {
                throw new RepositoryException(sprintf('%s: can\'t reregister built-in node type.', $type->getName()));
            }

            $nodeTypeName = $type->getName();

            $query = 'SELECT * FROM phpcr_type_nodes WHERE name = ?';
            $result = $this->getConnection()->fetchOne($query, [$nodeTypeName]);

            $data = [
                'name' => $nodeTypeName,
                'supertypes' => implode(' ', $type->getDeclaredSuperTypeNames()),
                'is_abstract' => $type->isAbstract() ? 1 : 0,
                'is_mixin' => $type->isMixin() ? 1 : 0,
                'queryable' => $type->isQueryable() ? 1 : 0,
                'orderable_child_nodes' => $type->hasOrderableChildNodes() ? 1 : 0,
                'primary_item' => $type->getPrimaryItemName(),
            ];

            if ($result) {
                if (!$allowUpdate) {
                    throw new NodeTypeExistsException("Could not register node type with the name '$nodeTypeName'.");
                }

                $this->getConnection()->update('phpcr_type_nodes', $data, ['node_type_id' => $result]);
                $this->getConnection()->delete('phpcr_type_props', ['node_type_id' => $result]);
                $this->getConnection()->delete('phpcr_type_childs', ['node_type_id' => $result]);

                $nodeTypeId = $result;
            } else {
                $this->getConnection()->insert('phpcr_type_nodes', $data);

                $nodeTypeId = $this->getConnection()->lastInsertId($this->sequenceTypeName);
            }

            if ($propDefs = $type->getDeclaredPropertyDefinitions()) {
                foreach ($propDefs as $propertyDef) {
                    /* @var $propertyDef PropertyDefinitionInterface */
                    $this->getConnection()->insert(
                        'phpcr_type_props',
                        [
                            'node_type_id' => $nodeTypeId,
                            'name' => $propertyDef->getName(),
                            'protected' => $propertyDef->isProtected() ? 1 : 0,
                            'mandatory' => $propertyDef->isMandatory() ? 1 : 0,
                            'auto_created' => $propertyDef->isAutoCreated() ? 1 : 0,
                            'on_parent_version' => $propertyDef->getOnParentVersion(),
                            'multiple' => $propertyDef->isMultiple() ? 1 : 0,
                            'fulltext_searchable' => $propertyDef->isFullTextSearchable() ? 1 : 0,
                            'query_orderable' => $propertyDef->isQueryOrderable() ? 1 : 0,
                            'required_type' => $propertyDef->getRequiredType(),
                            'query_operators' => 0, // transform to bitmask
                            'default_value' => $propertyDef->getDefaultValues() ? current(
                                $propertyDef->getDefaultValues()
                            ) : null,
                        ]
                    );
                }
            }

            if ($childDefs = $type->getDeclaredChildNodeDefinitions()) {
                foreach ($childDefs as $childDef) {
                    /* @var $childDef NodeDefinitionInterface */
                    $this->getConnection()->insert(
                        'phpcr_type_childs',
                        [
                            'node_type_id' => $nodeTypeId,
                            'name' => $childDef->getName(),
                            'protected' => $childDef->isProtected() ? 1 : 0,
                            'mandatory' => $childDef->isMandatory() ? 1 : 0,
                            'auto_created' => $childDef->isAutoCreated() ? 1 : 0,
                            'on_parent_version' => $childDef->getOnParentVersion(),
                            'primary_types' => implode(' ', $childDef->getRequiredPrimaryTypeNames() ?: []),
                            'default_type' => $childDef->getDefaultPrimaryTypeName(),
                        ]
                    );
                }
            }
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function setNodeTypeManager($nodeTypeManager): void
    {
        $this->nodeTypeManager = $nodeTypeManager;
    }

    /**
     * {@inheritDoc}
     */
    public function cloneFrom($srcWorkspace, $srcAbsPath, $destAbsPath, $removeExisting)
    {
        throw new NotImplementedException('Cloning nodes is not implemented yet');
    }

    /**
     * {@inheritDoc}
     */
    public function updateNode(Node $node, $srcWorkspace)
    {
        throw new NotImplementedException('Updating nodes is not implemented yet');
    }

    /**
     * {@inheritDoc}
     *
     * @throws RepositoryException when no binary data found
     */
    public function getBinaryStream($path)
    {
        $this->assertLoggedIn();

        $nodePath = PathHelper::getParentPath($path);
        $nodeId = $this->getSystemIdForNode($nodePath);
        $propertyName = PathHelper::getNodeName($path);

        $data = $this->getConnection()->fetchAllAssociative(
            'SELECT data, idx FROM phpcr_binarydata WHERE node_id = ? AND property_name = ? AND workspace_name = ?',
            [$nodeId, $propertyName, $this->workspaceName]
        );

        if (0 === count($data)) {
            throw new RepositoryException('No binary data found in stream');
        }

        $streams = [];
        foreach ($data as $row) {
            if (is_resource($row['data'])) {
                $stream = $row['data'];
            } else {
                $stream = fopen('php://memory', 'rwb+');
                fwrite($stream, $row['data']);
                rewind($stream);
            }

            $streams[] = $stream;
        }

        if (1 === count($data)) {
            // we don't know if this is a multivalue property or not.
            // TODO we should have something more efficient to know this. a flag in the database?

            // TODO use self::getProperty()->isMultiple() once implemented
            $node = $this->getNode($nodePath);
            if (!is_array($node->{':'.$propertyName})) {
                return reset($streams);
            }
        }

        return $streams;
    }

    /**
     * {@inheritDoc}
     */
    public function getProperty($path)
    {
        throw new NotImplementedException('Getting properties by path is not implemented yet');
    }

    /**
     * {@inheritDoc}
     *
     * @throws InvalidQueryException
     */
    public function query(Query $query)
    {
        $this->assertLoggedIn();

        if (!$query instanceof QueryObjectModelInterface) {
            $parser = new Sql2ToQomQueryConverter($this->factory->get(QueryObjectModelFactory::class));
            try {
                $qom = $parser->parse($query->getStatement());
                $qom->setLimit($query->getLimit());
                $qom->setOffset($query->getOffset());
            } catch (Exception $e) {
                throw new InvalidQueryException('Invalid query: '.$query->getStatement(), null, $e);
            }
        } else {
            $qom = $query;
        }

        $qomWalker = new QOMWalker($this->nodeTypeManager, $this->getConnection(), $this->getNamespaces());
        [$selectors, $selectorAliases, $sql] = $qomWalker->walkQOMQuery($qom);

        $primarySource = reset($selectors);
        $primaryType = $primarySource->getSelectorName() ?: $primarySource->getNodeTypeName();
        $statement = $this->getConnection()->executeQuery($sql, [$this->workspaceName]);

        $results = $properties = $standardColumns = [];
        while ($row = $statement->fetchAssociative()) {
            $result = [];

            /** @var SelectorInterface $selector */
            foreach ($selectors as $selector) {
                $selectorName = $selector->getSelectorName() ?: $selector->getNodeTypeName();
                $columnPrefix = isset($selectorAliases[$selectorName]) ? $selectorAliases[$selectorName].'_' : $selectorAliases[''].'_';

                if ($primaryType === $selector->getNodeTypeName()) {
                    $result[] = [
                        'dcr:name' => 'jcr:path',
                        'dcr:value' => $row[$columnPrefix.'path'],
                        'dcr:selectorName' => $selectorName,
                    ];
                }

                $result[] = [
                    'dcr:name' => 'jcr:path',
                    'dcr:value' => $row[$columnPrefix.'path'],
                    'dcr:selectorName' => $selectorName,
                ];
                $result[] = ['dcr:name' => 'jcr:score', 'dcr:value' => 0, 'dcr:selectorName' => $selectorName];
                if (0 === count($qom->getColumns())) {
                    $selectorPrefix = null !== $selector->getSelectorName() ? $selectorName.'.' : '';
                    $result[] = [
                        'dcr:name' => $selectorPrefix.'jcr:primaryType',
                        'dcr:value' => $primaryType,
                        'dcr:selectorName' => $selectorName,
                    ];
                }

                if (isset($row[$columnPrefix.'props'])) {
                    $propertyNames = [];
                    $columns = $qom->getColumns();

                    // Always populate jcr:created and jcr:createdBy if a wildcard selector is used.
                    // This emulates the behavior of Jackrabbit
                    if (0 === count($columns)) {
                        $propertyNames = ['jcr:created', 'jcr:createdBy'];
                    }

                    foreach ($columns as $column) {
                        if (!$column->getSelectorName() || $column->getSelectorName() == $selectorName) {
                            $propertyNames[] = $column->getPropertyName();
                        }
                    }

                    $properties[$selectorName] = (array) $this->xmlToColumns(
                        $row[$columnPrefix.'props'],
                        $propertyNames
                    );
                } else {
                    $properties[$selectorName] = [];
                }

                // TODO: add other default columns that Jackrabbit provides to provide a more consistent behavior
                if (isset($properties[$selectorName]['jcr:createdBy'])) {
                    $standardColumns[$selectorName]['jcr:createdBy'] = $properties[$selectorName]['jcr:createdBy'];
                }

                if (isset($properties[$selectorName]['jcr:created'])) {
                    $standardColumns[$selectorName]['jcr:created'] = $properties[$selectorName]['jcr:created'];
                }
            }

            $reservedNames = ['jcr:path', 'jcr:score'];

            foreach ($qom->getColumns() as $column) {
                $selectorName = $column->getSelectorName();
                $columnName = $column->getPropertyName();
                $columnPrefix = isset($selectorAliases[$selectorName]) ? $selectorAliases[$selectorName].'_' : $selectorAliases[''].'_';

                if (in_array($column->getColumnName(), $reservedNames)) {
                    throw new InvalidQueryException(
                        sprintf(
                            'Cannot reserved name "%s". Reserved names are "%s"',
                            $column->getColumnName(),
                            implode('", "', $reservedNames)
                        )
                    );
                }

                $dcrValue = 'jcr:uuid' === $columnName ? $row[$columnPrefix.'identifier'] : (isset($properties[$selectorName][$columnName]) ? $properties[$selectorName][$columnName] : '');

                if (isset($standardColumns[$selectorName][$columnName])) {
                    unset($standardColumns[$selectorName][$columnName]);
                }

                $result[] = [
                    'dcr:name' => $column->getColumnName(
                    ) === $columnName && isset($properties[$selectorName][$columnName]) ? $selectorName.'.'.$columnName : $column->getColumnName(
                    ),
                    'dcr:value' => $dcrValue,
                    'dcr:selectorName' => $selectorName ?: $primaryType,
                ];
            }

            foreach ($standardColumns as $selectorName => $columns) {
                foreach ($columns as $columnName => $value) {
                    $result[] = [
                        'dcr:name' => $primaryType.'.'.$columnName,
                        'dcr:value' => $value,
                        'dcr:selectorName' => $selectorName,
                    ];
                }
            }

            $results[] = $result;
        }

        return $results;
    }

    /**
     * {@inheritDoc}
     */
    public function getSupportedQueryLanguages(): array
    {
        return [
            QueryInterface::JCR_SQL2,
            QueryInterface::JCR_JQOM,
            QueryInterface::SQL,
        ];
    }

    /**
     * We need to create an in memory backup when we are inside a transaction
     * so that we can efficiently restore the original state in the namespaces
     * property in case of a rollback.
     *
     * This method also ensures that namespaces are loaded to begin with.
     */
    private function ensureNamespacesBackup(): void
    {
        if (!$this->namespaces instanceof \ArrayObject) {
            $this->getNamespacesObject();
        }

        if (!$this->inTransaction) {
            return;
        }

        if (null === $this->originalNamespaces) {
            $this->originalNamespaces = $this->namespaces->getArrayCopy();
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws NamespaceException
     */
    public function registerNamespace($prefix, $uri): void
    {
        if (isset($this->namespaces[$prefix])) {
            if ($this->namespaces[$prefix] === $uri) {
                return;
            }

            if (isset($this->coreNamespaces[$prefix])) {
                throw new NamespaceException(
                    "Cannot overwrite JCR core namespace prefix '$prefix' to a new uri '$uri'."
                );
            }
        }

        $this->ensureNamespacesBackup();

        $this->getConnection()->delete('phpcr_namespaces', ['prefix' => $prefix]);
        $this->getConnection()->delete('phpcr_namespaces', ['uri' => $uri]);

        $this->getConnection()->insert(
            'phpcr_namespaces',
            [
                'prefix' => $prefix,
                'uri' => $uri,
            ]
        );

        if (!empty($this->namespaces)) {
            $this->namespaces[$prefix] = $uri;
        }
    }

    /**
     * {@inheritDoc}
     */
    public function unregisterNamespace($prefix): void
    {
        if (isset($this->coreNamespaces[$prefix])) {
            throw new NamespaceException("Cannot unregister JCR core namespace prefix '$prefix'.");
        }

        $this->ensureNamespacesBackup();

        $this->getConnection()->delete('phpcr_namespaces', ['prefix' => $prefix]);

        if (!empty($this->namespaces)) {
            unset($this->namespaces[$prefix]);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function getReferences($path, $name = null): array
    {
        return $this->getNodeReferences($path, $name, false);
    }

    /**
     * {@inheritDoc}
     */
    public function getWeakReferences($path, $name = null): array
    {
        return $this->getNodeReferences($path, $name, true);
    }

    /**
     * @param string      $path          the path for which we need the references
     * @param string|null $name          the name of the referencing properties or null for all
     * @param bool        $weakReference whether to get weak or strong references
     *
     * @return array list of paths to nodes that reference $path
     */
    private function getNodeReferences(string $path, string $name = null, bool $weakReference = false): array
    {
        $targetId = $this->getSystemIdForNode($path);

        if (false === $targetId) {
            return [];
        }

        $params = [$targetId];

        $table = $weakReference ? $this->referenceTables[PropertyType::WEAKREFERENCE] : $this->referenceTables[PropertyType::REFERENCE];
        $query = "SELECT CONCAT(n.path, '/', r.source_property_name) FROM phpcr_nodes n
               INNER JOIN $table r ON n.id = r.source_id
               WHERE r.target_id = ?";
        if (null !== $name) {
            $query .= ' AND source_property_name = ?';
            $params[] = $name;
        }

        $stmt = $this->getConnection()->executeQuery($query, $params);

        return array_column($stmt->fetchAllNumeric(), 0);
    }

    /**
     * {@inheritDoc}
     *
     * @return ?string Transaction token if available
     */
    public function beginTransaction(): ?string
    {
        if ($this->inTransaction) {
            throw new RepositoryException('Begin transaction failed: transaction already open');
        }

        $this->assertLoggedIn();

        try {
            $this->getConnection()->beginTransaction();
            $this->inTransaction = true;
        } catch (Exception $e) {
            throw new RepositoryException('Begin transaction failed: '.$e->getMessage());
        }

        return null;
    }

    /**
     * {@inheritDoc}
     *
     * @throws RepositoryException
     */
    public function commitTransaction(): void
    {
        if (!$this->inTransaction) {
            throw new RepositoryException('Commit transaction failed: no transaction open');
        }

        $this->assertLoggedIn();

        try {
            $this->inTransaction = false;

            $this->getConnection()->commit();

            if ($this->originalNamespaces) {
                // now that the transaction is committed, reset the cache of the stored namespaces.
                $this->originalNamespaces = null;
            }
        } catch (Exception $e) {
            throw new RepositoryException('Commit transaction failed: '.$e->getMessage());
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws RepositoryException
     */
    public function rollbackTransaction(): void
    {
        if (!$this->inTransaction) {
            throw new RepositoryException('Rollback transaction failed: no transaction open');
        }

        $this->assertLoggedIn();

        try {
            $this->inTransaction = false;

            $this->getConnection()->rollBack();

            if ($this->originalNamespaces) {
                // reset namespaces
                $this->setNamespaces($this->originalNamespaces);
                $this->originalNamespaces = null;
            }
        } catch (Exception $e) {
            throw new RepositoryException('Rollback transaction failed: '.$e->getMessage(), 0, $e);
        }
    }

    /**
     * Sets the default transaction timeout.
     *
     * @param int $seconds The value of the timeout in seconds
     */
    public function setTransactionTimeout($seconds): void
    {
        $this->assertLoggedIn();

        throw new NotImplementedException('Setting a transaction timeout is not yet implemented');
    }

    /**
     * {@inheritDoc}
     */
    public function prepareSave(): void
    {
        $this->getConnection()->beginTransaction();
    }

    /**
     * {@inheritDoc}
     */
    public function finishSave(): void
    {
        $this->syncReferences($this->referencesToUpdate);
        $this->referencesToUpdate = $this->referencesToDelete = [];
        $this->getConnection()->commit();
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackSave(): void
    {
        $this->referencesToUpdate = $this->referencesToDelete = [];
        $this->getConnection()->rollBack();
    }

    /**
     * @param Node $node the node to update
     */
    public function updateProperties(Node $node): bool
    {
        $this->assertLoggedIn();
        // we can ignore the operations returned, there will be no additions because of property updates
        $this->getNodeProcessor()->process($node);

        $this->syncNode(
            $node->getIdentifier(),
            $node->getPath(),
            $node->getPrimaryNodeType()->getName(),
            false,
            $node->getProperties()
        );

        return true;
    }

    /**
     * Initialize the dbal connection lazily.
     */
    private function initConnection(): void
    {
        if ($this->connectionInitialized && $this->conn->isConnected()) {
            return;
        }

        $platform = $this->conn->getDatabasePlatform();
        if ($platform instanceof PostgreSQL94Platform || $platform instanceof PostgreSQLPlatform) {
            $this->sequenceNodeName = 'phpcr_nodes_id_seq';
            $this->sequenceTypeName = 'phpcr_type_nodes_node_type_id_seq';
        }

        // @TODO: move to "SqlitePlatform" and rename to "registerExtraFunctions"?
        if ($this->conn->getDatabasePlatform() instanceof SqlitePlatform) {
            $connection = $this->conn->getWrappedConnection();
            if ($connection instanceof PDOConnection && !$connection instanceof PDO) {
                $connection = $connection->getWrappedConnection();
            }

            $this->registerSqliteFunctions($connection);
        }

        $this->connectionInitialized = true;
    }

    private function determineDbalVersion(): void
    {
        $this->dbalVersion = method_exists(Statement::class, 'executeQuery') ? self::DBAL3 : self::DBAL2;
    }
}
