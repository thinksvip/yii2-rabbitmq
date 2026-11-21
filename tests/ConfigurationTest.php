<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests;

use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\exceptions\InvalidConfigException;
use yii\base\UnknownPropertyException;

class ConfigurationTest extends TestCase
{
    public function testUrlOverridesAreNormalizedBeforeConfigurationIsReturned()
    {
        $config = (new Configuration(['connections' => [[
            'name' => 'orders', 'url' => 'amqp://user%2Btag:p%2B%2Bword@broker:5672/%2Forders'
                . '?heartbeat=15&keepalive=false&read_write_timeout=35&channel_rpc_timeout=5&connection_timeout=1.25',
            'heartbeat' => 2, 'keepalive' => true,
        ]]]))->getConfig();
        $options = $config->connections[0];
        $this->assertSame('broker', $options['host']);
        $this->assertSame(5672, $options['port']);
        $this->assertSame('/orders', $options['vhost']);
        $this->assertSame('user+tag', $options['user']);
        $this->assertSame('p++word', $options['password']);
        $this->assertSame(15, $options['heartbeat']);
        $this->assertFalse($options['keepalive']);
        $this->assertSame(35, $options['read_write_timeout']);
        $this->assertSame(5, $options['channel_rpc_timeout']);
        $this->assertSame(1.25, $options['connection_timeout']);
        $this->assertNull($options['url']);
    }

    /** @dataProvider invalidConnectionOptions */
    public function testInvalidEffectiveConnectionOptionsFailDuringConfiguration(array $options, string $field)
    {
        try {
            (new Configuration(['connections' => [$options + ['name' => 'orders']]]))->getConfig();
            $this->fail('Expected connection configuration to fail before service construction.');
        } catch (InvalidConfigException $e) {
            $this->assertStringContainsString('orders', $e->getMessage());
            $this->assertStringContainsString($field, $e->getMessage());
            $this->assertStringNotContainsString('private-password', $e->getMessage());
            $this->assertStringNotContainsString('amqp://', $e->getMessage());
        }
    }

    public static function invalidConnectionOptions(): array
    {
        $url = 'amqp://user:private-password@broker/%2F?';
        return [
            'unknown query' => [['url' => $url . 'bogus=private-password'], 'url'],
            'nested query' => [['url' => $url . 'heartbeat[]=5'], 'url'],
            'metadata query' => [['url' => $url . 'name=another'], 'url'],
            'duplicate query' => [['url' => $url . 'heartbeat=5&heartbeat=10'], 'heartbeat'],
            'invalid boolean' => [['url' => $url . 'keepalive=off'], 'keepalive'],
            'negative heartbeat' => [['url' => $url . 'heartbeat=-5'], 'heartbeat'],
            'fractional heartbeat' => [['url' => $url . 'heartbeat=1.5'], 'heartbeat'],
            'heartbeat overflow' => [['url' => $url . 'heartbeat=65536'], 'heartbeat'],
            'invalid timeout' => [['url' => $url . 'connection_timeout=invalid'], 'connection_timeout'],
            'negative timeout' => [['url' => $url . 'read_write_timeout=-1'], 'read_write_timeout'],
            'infinite timeout' => [['url' => $url . 'connection_timeout=1e999'], 'connection_timeout'],
            'RPC exceeds I/O timeout' => [['url' => $url . 'channel_rpc_timeout=4&read_write_timeout=3'], 'channel_rpc_timeout'],
            'invalid effective port' => [['url' => $url . 'port=0'], 'port'],
            'malformed URL' => [['url' => 'amqp://user:private-password@broker:99999'], 'url'],
            'invalid percent escape' => [['url' => 'amqp://user:private-password%XX@broker'], 'url'],
            'URL without host' => [['url' => 'amqp:/orders'], 'url'],
            'empty URL' => [['url' => ''], 'url'],
            'URL of wrong type' => [['url' => ['private-password']], 'url'],
            'array negative heartbeat' => [['host' => 'broker', 'heartbeat' => -1], 'heartbeat'],
            'array boolean timeout' => [['host' => 'broker', 'connection_timeout' => false], 'connection_timeout'],
            'array port overflow' => [['host' => 'broker', 'port' => 65536], 'port'],
            'array invalid keepalive' => [['host' => 'broker', 'keepalive' => []], 'keepalive'],
        ];
    }

    public function testSingleConnectionShorthandAndNonconsecutiveIndex()
    {
        foreach ([['host' => 'localhost'], [4 => ['host' => 'localhost']]] as $connections) {
            $config = new Configuration(['connections' => $connections]);
            $this->assertSame($config, $config->getConfig());
            $this->assertSame($config, $config->getConfig());
            $this->assertCount(1, $config->connections);
            $connection = reset($config->connections);
            $this->assertSame('default', $connection['name']);
            $this->assertSame('localhost', $connection['host']);
        }
    }

    /** @dataProvider invalidOptions */
    public function testReportsTheOffendingOption(array $config, string $message)
    {
        $this->expectException(InvalidConfigException::class);
        $this->expectExceptionMessage($message);
        (new Configuration($config + ['connections' => [['host' => 'localhost']]]))->getConfig();
    }

    public static function invalidOptions()
    {
        $connection = ['host' => 'localhost'];
        yield 'connections is scalar' => [['connections' => 'invalid'], 'connections'];
        yield 'scalar entry' => [['connections' => [1]], 'connection entry'];
        yield 'invalid list index' => [['queues' => ['key' => []]], 'Invalid key'];
        yield 'scalar list' => [['queues' => 'invalid'], 'queue entry'];
        yield 'TLS options' => [['connections' => [$connection + ['ssl_context' => false]]], 'ssl_context'];
        yield 'TLS over socket' => [['connections' => [$connection + [
            'ssl_context' => [], 'type' => \PhpAmqpLib\Connection\AMQPSocketConnection::class,
        ]]], 'stream connection'];
        yield 'unknown resource connection' => [['queues' => [['name' => 'jobs', 'connection' => 'missing']]], 'Unknown connection'];
        $resources = ['exchanges' => [['name' => 'events', 'type' => 'topic']], 'queues' => [['name' => 'jobs']]];
        yield 'unknown exchange type' => [['exchanges' => [['name' => 'events', 'type' => 'bad']]], 'Unknown exchange type'];
        foreach ([
            'routing_keys' => [['routing_keys' => 'bad', 'queue' => 'jobs'], 'routing_keys'],
            'neither destination' => [[], 'to_exchange'],
            'both destinations' => [['to_exchange' => 'other', 'queue' => 'jobs'], 'to_exchange'],
            'unknown queue' => [['queue' => 'missing'], 'missing'],
        ] as $case => [$binding, $message]) {
            yield $case => [$resources + ['bindings' => [['exchange' => 'events'] + $binding]], $message];
        }
        foreach (['callbacks' => 'invalid', 'qos' => 0, 'proceed_on_exception' => 0, 'deserializer' => 0] as $key => $value) {
            $consumer = [$key => $value, 'name' => 'worker'] + ['callbacks' => ['jobs' => 'handler']];
            yield $key => [$resources + ['consumers' => [$consumer]], $key];
        }
        yield 'missing default connection' => [[
            'connections' => [['name' => 'a'] + $connection], 'consumers' => [['name' => 'worker']],
        ], 'Connection for consumer'];
    }

    public function testIsNotConfigured()
    {
        $this->expectException(UnknownPropertyException::class);
        $this->mockApplication();
        \Yii::$app->rabbitmq->getConfig();
    }

    public function testConfigType()
    {
        $this->mockApplication([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'host' => 'localhost',
                        ],
                    ],
                ],
            ],
        ]);
        $this->assertInstanceOf(Configuration::class, \Yii::$app->rabbitmq->getConfig());
    }

    /**
     * @dataProvider invalidConfig
     */
    public function testInvalidConfig($reason, $config, $exception)
    {
        $this->expectException($exception);
        $rabbitmq = array_merge(['class' => Configuration::class], $config);
        $components = [
            'components' => [
                'rabbitmq' => $rabbitmq,
            ],
        ];
        $this->mockApplication($components);
        \Yii::$app->rabbitmq->getConfig();
    }

    public function invalidConfig()
    {
        $required = ['connections' => [['host' => 'localhost']]];

        return [
            ['At least one connection required', ['connections' => []], InvalidConfigException::class],
            ['Unknown option', array_merge($required, ['unknown' => []]), UnknownPropertyException::class],
            ['Name should be specified on multiple connections', ['connections' => [['host' => 'rabbitmq'],['host' => 'rabbitmq']]], InvalidConfigException::class],
            ['Wrong auto-declare option', array_merge($required, ['auto_declare' => 43]), InvalidConfigException::class],
            ['Logger is not an array', array_merge($required, ['logger' => 43]), InvalidConfigException::class],
            ['Unknown option in logger section', array_merge($required, ['logger' => ['unknown' => 43]]), InvalidConfigException::class],
            ['Connections array should be multidimensional and numeric', ['connections' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Bindings array should be multidimensional and numeric', ['bindings' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Exchanges array should be multidimensional and numeric', ['exchanges' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Queues array should be multidimensional and numeric', ['queues' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Producers array should be multidimensional and numeric', ['producers' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Consumers array should be multidimensional and numeric', ['consumers' => ['some-key' => 'some-value']], InvalidConfigException::class],
            ['Url or host is required', ['connections' => [[]]], InvalidConfigException::class],
            ['Not both url and host allowed', ['connections' => [['url' => 'some-value1', 'host' => 'some-value2']]], InvalidConfigException::class],
            ['No additional fields are allowed in connections', ['connections' => [['wrong' => 'wrong', 'host' => 'localhost']]], InvalidConfigException::class],
            ['Connection type should be of correct subclass', ['connections' => [['type' => 'SomeTypeUnknown', 'host' => 'localhost']]], InvalidConfigException::class],
            ['Exchange name is required', array_merge($required, ['exchanges' => [['type' => 'direct']]]), InvalidConfigException::class],
            ['Exchange type is required', array_merge($required, ['exchanges' => [['name' => 'direct']]]), InvalidConfigException::class],
            ['Exchange type should be one of allowed', array_merge($required, ['exchanges' => [['type' => 'wrong']]]), InvalidConfigException::class],
            ['Exchange wrong field', array_merge($required, ['exchanges' => [['type' => 'direct', 'name' => 'direct', 'wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Queue wrong field', array_merge($required, ['queues' => [['wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Exchange name is required for binding', array_merge($required, ['bindings' => [[]]]), InvalidConfigException::class],
            ['Routing key is required for binding', array_merge($required, ['bindings' => [['exchange' => 'smth']]]), InvalidConfigException::class],
            ['Either `queue` or `to_exchange` options should be specified to create binding', array_merge($required, ['bindings' => [['exchange' => 'smth', 'routing_keys' => ['smth'],]]]), InvalidConfigException::class],
            ['Exchanges and queues should be configured in corresponding sections', array_merge($required, ['bindings' => [['exchange' => 'smth', 'routing_keys' => ['smth'], 'queue' => 'smth',]]]), InvalidConfigException::class],
            ['Binding wrong field', array_merge($required, ['bindings' => [['wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Producer wrong field', array_merge($required, ['producers' => [['wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Producer name is required', array_merge($required, ['producers' => [[]]]), InvalidConfigException::class],
            ['Connection defined in producer should exist', array_merge($required, ['producers' => [['name' => 'smth', 'connection' => 'unknown']]]), InvalidConfigException::class],
            ['Safe option should be a boolean', array_merge($required, ['producers' => [['name' => 'smth', 'safe' => 'non_bool']]]), InvalidConfigException::class],
            ['Serializer should be callable', array_merge($required, ['producers' => [['name' => 'smth', 'serializer' => 'non_callable']]]), InvalidConfigException::class],
            ['Consumer wrong field', array_merge($required, ['consumers' => [['wrong' => 'wrong']]]), InvalidConfigException::class],
            ['Consumer name is required', array_merge($required, ['consumers' => [[]]]), InvalidConfigException::class],
            ['Connection defined in consumer should exist', array_merge($required, ['consumers' => [['name' => 'smth', 'connection' => 'unknown']]]), InvalidConfigException::class],
            ['No callbacks specified in consumer', array_merge($required, ['consumers' => [['name' => 'smth', 'callbacks' => []]]]), InvalidConfigException::class],
            ['Option qos is not an array', array_merge($required, ['consumers' => [['name' => 'smth', 'qos' => 'not_an_array']]]), InvalidConfigException::class],
            ['Option proceed_on_exception is not a boolean', array_merge($required, ['consumers' => [['name' => 'smth', 'proceed_on_exception' => 'not_a_boolean']]]), InvalidConfigException::class],
            ['Queue defined in consumer should exist', array_merge($required, ['consumers' => [['name' => 'smth', 'callbacks' => ['unknown' => 'some-callback']]]]), InvalidConfigException::class],
            ['Consumer callback parameter should be string', array_merge($required, ['queues' => [['name' => 'smth']], 'consumers' => [['name' => 'smth', 'callbacks' => ['smth' => 34]]]]), InvalidConfigException::class],
            ['Deserializer should be callable', array_merge($required, ['consumers' => [['name' => 'smth', 'deserializer' => 'non_callable']]]), InvalidConfigException::class],
            ['Connection in consumer not exist', ['connections' => [['host' => 'rabbitmq']], 'consumers' => [['name' => 'smth', 'connection' => 'default2']]], InvalidConfigException::class],
            ['Named connection not specified in producer', ['connections' => [['host' => 'rabbitmq', 'name' => 'default2']], 'producers' => [['name' => 'smth']]], InvalidConfigException::class],
            ['Duplicate names in producer', array_merge($required, ['producers' => [['name' => 'smth'], ['name' => 'smth']]]), InvalidConfigException::class],
        ];
    }
}
