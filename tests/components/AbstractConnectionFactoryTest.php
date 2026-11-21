<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\AbstractConnectionFactory;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPLazySocketConnection;
use PhpAmqpLib\Connection\AMQPLazySSLConnection;

class AbstractConnectionFactoryTest extends TestCase
{
    public function testCreateConnection() {
        $testOptions = [
            'name' => 'test',
            'url' => null,
            'host' => 'localhost',
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
        ];

        $factory = new AbstractConnectionFactory(AMQPLazyConnection::class, $testOptions);
        $connection = $factory->createConnection();
        $this->assertInstanceOf(AbstractConnection::class, $connection);
    }

    /**
     * @dataProvider lazyTlsTypes
     */
    public function testTlsOptionsReachTheLazyStream($type)
    {
        $parameters = Configuration::DEFAULTS['connections'][0];
        $parameters['host'] = 'localhost';
        $parameters['ssl_context'] = [
            'peer_name' => 'broker.example',
            'verify_peer' => true,
            'cafile' => '/path/to/ca.pem',
        ];

        $connection = (new AbstractConnectionFactory($type, $parameters))->createConnection();
        $this->assertFalse($connection->isConnected());
        $context = $this->getInaccessibleProperty($connection->getIO(), 'context');
        $this->assertSame(
            $parameters['ssl_context'] + ['crypto_method' => STREAM_CRYPTO_METHOD_TLS_CLIENT],
            stream_context_get_options($context)['ssl']
        );
    }

    public static function lazyTlsTypes(): array
    {
        return [[AMQPLazyConnection::class], [AMQPLazySSLConnection::class]];
    }

    public function testSocketRejectsTlsBeforeOpeningAConnection()
    {
        $parameters = Configuration::DEFAULTS['connections'][0];
        $parameters['host'] = 'localhost';
        $parameters['ssl_context'] = ['verify_peer' => true];
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Socket connections do not support TLS options.');
        (new AbstractConnectionFactory(AMQPLazySocketConnection::class, $parameters))->createConnection();
    }

    public function testCustomTlsConstructorReceivesCredentialsAndOptions()
    {
        $fixture = new class extends AMQPLazyConnection {
            public $arguments;

            public function __construct(...$arguments)
            {
                $this->arguments = $arguments;
            }
        };
        $transport = [
            'connection_timeout' => 3.5, 'read_write_timeout' => 12, 'keepalive' => true,
            'heartbeat' => 5, 'channel_rpc_timeout' => 2.5,
        ];
        $ssl = ['cafile' => '/test-ca.pem'];
        $parameters = ['url' => 'amqp://user:pass@broker:5671/%2Fteam', 'ssl_context' => $ssl] + $transport;
        $this->loadExtension(['components' => ['rabbitmq' => [
            'class' => Configuration::class,
            'connections' => [$parameters + ['type' => get_class($fixture)]],
        ]]]);
        $connection = \Yii::$app->rabbitmq->getConnection();
        $this->assertSame(['broker', 5671, 'user', 'pass', '/team', $ssl, $transport], $connection->arguments);
    }

    public function testSocketConnectionUsesTheConfiguredTimeouts()
    {
        $parameters = Configuration::DEFAULTS['connections'][0];
        $parameters['host'] = 'localhost';
        $parameters['read_write_timeout'] = 17.5;
        $parameters['keepalive'] = true;
        $parameters['heartbeat'] = 7;
        $parameters['channel_rpc_timeout'] = 2.5;

        $connection = (new AbstractConnectionFactory(AMQPLazySocketConnection::class, $parameters))->createConnection();
        $this->assertInstanceOf(AMQPLazySocketConnection::class, $connection);
        $this->assertFalse($connection->isConnected());
        $this->assertSame(17.5, $connection->getReadTimeout());
        $this->assertSame(17.5, $this->getInaccessibleProperty($connection->getIO(), 'write_timeout'));
        $this->assertTrue($this->getInaccessibleProperty($connection->getIO(), 'keepalive'));
        $this->assertSame(7, $connection->getHeartbeat());
        $this->assertSame(2.5, $this->getInaccessibleProperty($connection, 'channel_rpc_timeout'));
    }

    /**
     * @dataProvider vhostPaths
     */
    public function testConnectionUrlPreservesVhostAndCredentials($path, $expectedVhost)
    {
        $parameters = Configuration::DEFAULTS['connections'][0];
        $parameters['url'] = 'amqp://user+tag:p%2B+word@localhost' . $path;
        $factory = new AbstractConnectionFactory(AMQPLazyConnection::class, $parameters);
        $resolved = $this->getInaccessibleProperty($factory, '_parameters');
        $this->assertSame($expectedVhost, $resolved['vhost']);
        $this->assertSame('user+tag', $resolved['user']);
        $this->assertSame('p++word', $resolved['password']);
    }

    public static function vhostPaths(): array
    {
        return [
            ['', '/'],
            ['/%2F', '/'],
            ['/orders+events', 'orders+events'],
            ['/%2Fteam%2Forders', '/team/orders'],
            ['//team', '/team'],
        ];
    }

    /** @dataProvider urlKeepaliveValues */
    public function testUrlOptionsReachTheTransportWithTheirIntendedTypes(string $keepalive, bool $expected)
    {
        $connection = (new AbstractConnectionFactory(AMQPLazyConnection::class, [
            'url' => 'amqp://user:pass@localhost/%2F?keepalive=' . $keepalive
                . '&heartbeat=5&read_write_timeout=12.5&connection_timeout=1.5&channel_rpc_timeout=2',
        ]))->createConnection();

        $this->assertFalse($connection->isConnected());
        $this->assertSame($expected, $this->getInaccessibleProperty($connection->getIO(), 'keepalive'));
        $this->assertSame(5, $connection->getHeartbeat());
        $this->assertSame(12.5, $connection->getReadTimeout());
        $this->assertSame(1.5, $this->getInaccessibleProperty($connection, 'connection_timeout'));
        $this->assertSame(2, $this->getInaccessibleProperty($connection, 'channel_rpc_timeout'));
    }

    public static function urlKeepaliveValues(): array
    {
        return [['false', false], ['0', false], ['true', true], ['1', true]];
    }
}
