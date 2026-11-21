<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq;

use mikemadisonweb\rabbitmq\components\AbstractConnectionFactory;
use mikemadisonweb\rabbitmq\components\Consumer;
use mikemadisonweb\rabbitmq\components\Producer;
use mikemadisonweb\rabbitmq\components\Routing;
use mikemadisonweb\rabbitmq\exceptions\InvalidConfigException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use Yii;
use yii\base\Component;
use yii\di\NotInstantiableException;
use yii\helpers\ArrayHelper;

class Configuration extends Component
{
    const CONNECTION_SERVICE_NAME = 'rabbit_mq.connection.%s';
    const CONSUMER_SERVICE_NAME = 'rabbit_mq.consumer.%s';
    const PRODUCER_SERVICE_NAME = 'rabbit_mq.producer.%s';
    const ROUTING_SERVICE_NAME = 'rabbit_mq.routing';
    const LOGGER_SERVICE_NAME = 'rabbit_mq.logger';

    const DEFAULT_CONNECTION_NAME = 'default';
    const EXTENSION_CONTROLLER_ALIAS = 'rabbitmq';
    /**
     * Extension configuration default values
     * @var array
     */
    const DEFAULTS = [
        'auto_declare' => true,
        'connections' => [
            [
                'name' => self::DEFAULT_CONNECTION_NAME,
                'type' => AMQPLazyConnection::class,
                'url' => null,
                'host' => null,
                'port' => 5672,
                'user' => 'guest',
                'password' => 'guest',
                'vhost' => '/',
                'connection_timeout' => 3,
                'read_write_timeout' => 3,
                'ssl_context' => null,
                'keepalive' => false,
                'heartbeat' => 0,
                'channel_rpc_timeout' => 0.0,
            ],
        ],
        'exchanges' => [
            [
                'name' => null,
                'connection' => null,
                'type' => null,
                'passive' => false,
                'durable' => true,
                'auto_delete' => false,
                'internal' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null,
                'declare' => true,
            ],
        ],
        'queues' => [
            [
                'name' => '',
                'connection' => null,
                'passive' => false,
                'durable' => true,
                'exclusive' => false,
                'auto_delete' => false,
                'nowait' => false,
                'arguments' => null,
                'ticket' => null,
                'declare' => true,
            ],
        ],
        'bindings' => [
            [
                'exchange' => null,
                'connection' => null,
                'queue' => null,
                'to_exchange' => null,
                'routing_keys' => [],
            ],
        ],
        'producers' => [
            [
                'name' => null,
                'connection' => self::DEFAULT_CONNECTION_NAME,
                'safe' => true,
                'content_type' => 'text/plain',
                'delivery_mode' => 2,
                'serializer' => 'serialize',
            ],
        ],
        'consumers' => [
            [
                'name' => null,
                'connection' => self::DEFAULT_CONNECTION_NAME,
                'callbacks' => [],
                'qos' => [
                    'prefetch_size' => 0,
                    'prefetch_count' => 0,
                    'global' => false,
                ],
                'idle_timeout' => 0,
                'idle_timeout_exit_code' => null,
                'proceed_on_exception' => false,
                'deserializer' => 'unserialize',
                'systemd' => [
                    'memory_limit' => 0,
                    'workers' => 1,
                    'timeout_stop_sec' => 300,
                    'restart_sec' => 5,
                ],
            ],
        ],
        'logger' => [
            'log' => false,
            'category' => 'application',
            'print_console' => true,
            'system_memory' => false,
        ],
    ];

    public $auto_declare = null;
    public $connections = [];
    public $producers = [];
    public $consumers = [];
    public $queues = [];
    public $exchanges = [];
    public $bindings = [];
    public $logger = [];

    protected $isLoaded = false;

    /**
     * Get passed configuration
     * @return Configuration
     * @throws InvalidConfigException
     */
    public function getConfig() : Configuration
    {
        if(!$this->isLoaded) {
            $this->normalizeConnections();
            $this->validate();
            $this->completeWithDefaults();
            $this->isLoaded = true;
        }

        return $this;
    }

    /**
     * Get connection service
     * @param string $connectionName
     * @return object|AbstractConnection
     * @throws NotInstantiableException
     * @throws \yii\base\InvalidConfigException
     */
    public function getConnection(string $connectionName = '') : AbstractConnection
    {
        if ('' === $connectionName) {
            $connectionName = self::DEFAULT_CONNECTION_NAME;
        }

        return Yii::$container->get(sprintf(self::CONNECTION_SERVICE_NAME, $connectionName));
    }

    /**
     * Get producer service
     * @param string $producerName
     * @return Producer|object
     * @throws \yii\base\InvalidConfigException
     * @throws NotInstantiableException
     */
    public function getProducer(string $producerName)
    {
        return Yii::$container->get(sprintf(self::PRODUCER_SERVICE_NAME, $producerName));
    }

    /**
     * Get consumer service
     * @param string $consumerName
     * @return Consumer|object
     * @throws NotInstantiableException
     * @throws \yii\base\InvalidConfigException
     */
    public function getConsumer(string $consumerName)
    {
        return Yii::$container->get(sprintf(self::CONSUMER_SERVICE_NAME, $consumerName));
    }

    /**
     * Get routing service
     * @param AbstractConnection $connection
     * @return Routing|object|string
     * @throws NotInstantiableException
     * @throws \yii\base\InvalidConfigException
     */
    public function getRouting(AbstractConnection $connection)
    {
        return Yii::$container->get(Configuration::ROUTING_SERVICE_NAME, ['conn' => $connection]);
    }

    /**
     * Unscoped entries are shared. A binding also requires both ends in the selected vhost.
     * A null connection name selects shared entries for an externally supplied connection.
     */
    public function getRoutingConfiguration(?string $connectionName): array
    {
        $matchesConnection = static function ($entry) use ($connectionName) {
            return !isset($entry['connection']) || $entry['connection'] === $connectionName;
        };
        $queues = array_filter($this->queues, $matchesConnection);
        $exchanges = array_filter($this->exchanges, $matchesConnection);
        $bindings = [];
        foreach (array_filter($this->bindings, $matchesConnection) as $index => $binding) {
            if (!$this->isNameExist($exchanges, $binding['exchange'])) {
                continue;
            }
            if (isset($binding['queue']) && !$this->isNameExist($queues, $binding['queue'])) {
                continue;
            }
            // The destination exchange can also be managed outside this application.
            if (isset($binding['to_exchange']) && $this->isNameExist($this->exchanges, $binding['to_exchange'])
                && !$this->isNameExist($exchanges, $binding['to_exchange'])) {
                continue;
            }
            $bindings[$index] = $binding;
        }

        return compact('queues', 'exchanges', 'bindings');
    }

    /**
     * Config validation
     * @throws InvalidConfigException
     */
    protected function validate()
    {
        $this->validateTopLevel();
        $this->validateMultidimensional();
        $this->validateRequired();
        $this->validateDuplicateNames(['connections', 'producers', 'consumers']);
        $this->validateRouting();
    }

    /**
     * Validate multidimensional entries names
     * @throws InvalidConfigException
     */
    protected function validateMultidimensional()
    {
        $multidimensional = [
            'connection' => $this->connections,
            'exchange' => $this->exchanges,
            'queue' => $this->queues,
            'binding' => $this->bindings,
            'producer' => $this->producers,
            'consumer' => $this->consumers,
        ];

        foreach ($multidimensional as $configName => $configItem) {
            if (!is_array($configItem)) {
                throw new InvalidConfigException("Every {$configName} entry should be of type array.");
            }
            foreach ($configItem as $key => $value) {
                if (!is_int($key)) {
                    throw new InvalidConfigException("Invalid key: `{$key}`. There should be a list of {$configName}s in the array.");
                }
                if (!is_array($value)) {
                    throw new InvalidConfigException("Every {$configName} entry should be of type array.");
                }
            }
        }
    }

    /**
     * Validate top level options
     * @throws InvalidConfigException
     */
    protected function validateTopLevel()
    {
        if (($this->auto_declare !== null) && !is_bool($this->auto_declare)) {
            throw new InvalidConfigException("Option `auto_declare` should be of type boolean.");
        }

        if (!is_array($this->logger)) {
            throw new InvalidConfigException("Option `logger` should be of type array.");
        }

        $this->validateArrayFields($this->logger, self::DEFAULTS['logger']);
    }

    /**
     * Validate required options
     * @throws InvalidConfigException
     */
    protected function validateRequired()
    {
        foreach ($this->connections as $index => $connection) {
            $this->validateArrayFields($connection, self::DEFAULTS['connections'][0]);
            if (!isset($connection['url']) && !isset($connection['host'])) {
                throw new InvalidConfigException('Either `url` or `host` options required for configuring connection.');
            }
            if (isset($connection['url']) && (isset($connection['host']) || isset($connection['port']))) {
                throw new InvalidConfigException('Connection options `url` and `host:port` should not be both specified, configuration is ambigious.');
            }
            if (!isset($connection['name'])) {
                throw new InvalidConfigException('Connection name is required when multiple connections is specified.');
            }
            if (isset($connection['type']) && !is_subclass_of($connection['type'], AbstractConnection::class)) {
                throw new InvalidConfigException('Connection type should be a subclass of PhpAmqpLib\Connection\AbstractConnection.');
            }
            if (isset($connection['ssl_context'])) {
                if (!is_array($connection['ssl_context'])) {
                    throw new InvalidConfigException('Connection option `ssl_context` must be an array of TLS options.');
                }
                $type = $connection['type'] ?? self::DEFAULTS['connections'][0]['type'];
                if (!is_a($type, AMQPStreamConnection::class, true)) {
                    throw new InvalidConfigException('TLS options require a stream connection.');
                }
            }
            $this->connections[$index] = AbstractConnectionFactory::normalizeParameters($connection);
        }

        foreach ($this->exchanges as $exchange) {
            $this->validateArrayFields($exchange, self::DEFAULTS['exchanges'][0]);
            if (!isset($exchange['name'])) {
                throw new InvalidConfigException('Exchange name should be specified.');
            }
            if (!isset($exchange['type'])) {
                throw new InvalidConfigException('Exchange type should be specified.');
            }
            $allowed = ['direct', 'topic', 'fanout', 'headers'];
            if (!in_array($exchange['type'], $allowed, true)) {
                $allowed = implode(', ', $allowed);
                throw new InvalidConfigException("Unknown exchange type `{$exchange['type']}`. Allowed values are: {$allowed}");
            }
        }
        foreach ($this->queues as $queue) {
            $this->validateArrayFields($queue, self::DEFAULTS['queues'][0]);
        }
        foreach ($this->bindings as $binding) {
            $this->validateArrayFields($binding, self::DEFAULTS['bindings'][0]);
            if (!isset($binding['exchange'])) {
                throw new InvalidConfigException('Exchange name is required for binding.');
            }
            if (!$this->isNameExist($this->exchanges, $binding['exchange'])) {
                throw new InvalidConfigException("`{$binding['exchange']}` defined in binding doesn't configured in exchanges.");
            }
            if (isset($binding['routing_keys']) && !is_array($binding['routing_keys'])) {
                throw new InvalidConfigException('Option `routing_keys` should be an array.');
            }
            if ((!isset($binding['queue']) && !isset($binding['to_exchange'])) || isset($binding['queue'], $binding['to_exchange'])) {
                throw new InvalidConfigException('Either `queue` or `to_exchange` options should be specified to create binding.');
            }
            if (isset($binding['queue']) && !$this->isNameExist($this->queues, $binding['queue'])) {
                throw new InvalidConfigException("`{$binding['queue']}` defined in binding doesn't configured in queues.");
            }
        }
        foreach ($this->producers as $producer) {
            $this->validateArrayFields($producer, self::DEFAULTS['producers'][0]);
            if (!isset($producer['name'])) {
                throw new InvalidConfigException('Producer name is required.');
            }
            if (isset($producer['connection']) && !$this->isNameExist($this->connections, $producer['connection'])) {
                throw new InvalidConfigException("Connection `{$producer['connection']}` defined in producer doesn't configured in connections.");
            }
            if (isset($producer['safe']) && !is_bool($producer['safe'])) {
                throw new InvalidConfigException('Producer option safe should be of type boolean.');
            }
            if (!isset($producer['connection']) && !$this->isNameExist($this->connections, self::DEFAULT_CONNECTION_NAME)) {
                throw new InvalidConfigException("Connection for producer `{$producer['name']}` is required.");
            }
            if (isset($producer['serializer']) && !is_callable($producer['serializer'])) {
                throw new InvalidConfigException('Producer `serializer` option should be a callable.');
            }
        }
        foreach ($this->consumers as $consumer) {
            $this->validateArrayFields($consumer, self::DEFAULTS['consumers'][0]);
            if (!isset($consumer['name'])) {
                throw new InvalidConfigException('Consumer name is required.');
            }
            if (isset($consumer['connection']) && !$this->isNameExist($this->connections, $consumer['connection'])) {
                throw new InvalidConfigException("Connection `{$consumer['connection']}` defined in consumer doesn't configured in connections.");
            }
            if (!isset($consumer['connection']) && !$this->isNameExist($this->connections, self::DEFAULT_CONNECTION_NAME)) {
                throw new InvalidConfigException("Connection for consumer `{$consumer['name']}` is required.");
            }
            if (!isset($consumer['callbacks']) || empty($consumer['callbacks'])) {
                throw new InvalidConfigException("No callbacks specified for consumer `{$consumer['name']}`.");
            }
            if (!is_array($consumer['callbacks'])) {
                throw new InvalidConfigException('Consumer option `callbacks` should be of type array.');
            }
            if (isset($consumer['qos']) && !is_array($consumer['qos'])) {
                throw new InvalidConfigException('Consumer option `qos` should be of type array.');
            }
            if (isset($consumer['proceed_on_exception']) && !is_bool($consumer['proceed_on_exception'])) {
                throw new InvalidConfigException('Consumer option `proceed_on_exception` should be of type boolean.');
            }
            if (array_key_exists('systemd', $consumer)) {
                if (!is_array($consumer['systemd'])) {
                    throw new InvalidConfigException('Consumer option `systemd` should be of type array.');
                }
                $this->validateArrayFields($consumer['systemd'], self::DEFAULTS['consumers'][0]['systemd']);
                foreach (['timeout_stop_sec', 'restart_sec'] as $key) {
                    if (array_key_exists($key, $consumer['systemd'])
                        && (!is_int($consumer['systemd'][$key]) || $consumer['systemd'][$key] <= 0)) {
                        throw new InvalidConfigException("Consumer systemd option `{$key}` must be a positive integer number of seconds.");
                    }
                }
            }
            $connectionName = $consumer['connection'] ?? self::DEFAULT_CONNECTION_NAME;
            foreach ($consumer['callbacks'] as $queue => $callback) {
                if (!$this->isNameExist($this->queues, (string)$queue, $connectionName)) {
                    throw new InvalidConfigException("Queue `{$queue}` from {$consumer['name']} is not defined on connection `{$connectionName}`.");
                }
                if (!is_string($callback)) {
                    throw new InvalidConfigException('Consumer `callback` parameter value should be a class name or service name in DI container.');
                }
            }
            if (isset($consumer['deserializer']) && !is_callable($consumer['deserializer'])) {
                throw new InvalidConfigException('Consumer `deserializer` option should be a callable.');
            }
        }
    }

    /**
     * Validate config entry value
     * @param array $passed
     * @param array $required
     * @throws InvalidConfigException
     */
    protected function validateArrayFields(array $passed, array $required)
    {
        $undeclaredFields = array_diff_key($passed, $required);
        if (!empty($undeclaredFields)) {
            $asString = json_encode($undeclaredFields);
            throw new InvalidConfigException("Unknown options: {$asString}");
        }
    }

    private function validateRouting()
    {
        foreach (['exchanges', 'queues', 'bindings'] as $key) {
            foreach ($this->$key as $entry) {
                if (isset($entry['connection']) && (!is_string($entry['connection'])
                    || !$this->isNameExist($this->connections, $entry['connection']))) {
                    throw new InvalidConfigException("Unknown connection in {$key}.");
                }
            }
        }
        $applicableBindings = [];
        foreach ($this->connections as $connection) {
            $routing = $this->getRoutingConfiguration($connection['name']);
            $this->checkUniqueNames($routing['exchanges'], 'exchanges');
            $this->checkUniqueNames($routing['queues'], 'queues');
            $applicableBindings += $routing['bindings'];
        }
        if (count($applicableBindings) !== count($this->bindings)) {
            throw new InvalidConfigException('Binding endpoints must be available on the same connection.');
        }
    }

    protected function validateDuplicateNames(array $keys)
    {
        foreach ($keys as $key) {
            $this->checkUniqueNames($this->$key, $key);
        }
    }

    private function checkUniqueNames(array $items, string $key)
    {
        $names = [];
        foreach ($items as $item) {
            $name = $item['name'] ?? '';
            if (isset($names[$name])) {
                throw new InvalidConfigException("Duplicate name `{$name}` in {$key}");
            }
            $names[$name] = true;
        }
    }

    /**
     * Allow certain flexibility on connection configuration
     * @throws InvalidConfigException
     */
    protected function normalizeConnections()
    {
        if (!is_array($this->connections) || !$this->connections) {
            throw new InvalidConfigException('Option `connections` should have at least one entry.');
        }
        if (ArrayHelper::isAssociative($this->connections)) {
            $this->connections = [$this->connections];
        }
        if (count($this->connections) === 1) {
            $key = array_key_first($this->connections);
            if (is_array($this->connections[$key]) && !isset($this->connections[$key]['name'])) {
                $this->connections[$key]['name'] = self::DEFAULT_CONNECTION_NAME;
            }
        }
    }

    /**
     * Merge passed config with extension defaults
     */
    protected function completeWithDefaults()
    {
        $defaults = self::DEFAULTS;
        if (null === $this->auto_declare) {
            $this->auto_declare = $defaults['auto_declare'];
        }
        if (empty($this->logger)) {
            $this->logger = $defaults['logger'];
        } else {
            foreach ($defaults['logger'] as $key => $option) {
                if (!isset($this->logger[$key])) {
                    $this->logger[$key] = $option;
                }
            }
        }
        $multi = ['connections', 'bindings', 'exchanges', 'queues', 'producers', 'consumers'];
        foreach ($multi as $key) {
            foreach ($this->$key as &$item) {
                $item = array_replace_recursive($defaults[$key][0], $item);
            }
        }
    }

    /**
     * Check if an entry with specific name exists in array
     * @param array $items
     * @param string $name
     * @param string|null $connectionName
     * @return bool
     */
    private function isNameExist(array $items, string $name, ?string $connectionName = null)
    {
        foreach ($items as $item) {
            if (($item['name'] ?? '') === $name && ($connectionName === null
                || !isset($item['connection']) || $item['connection'] === $connectionName)) {
                return true;
            }
        }

        return false;
    }
}
