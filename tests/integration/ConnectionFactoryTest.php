<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\integration;

use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPLazySocketConnection;
use PhpAmqpLib\Connection\AMQPLazySSLConnection;
use PhpAmqpLib\Connection\AMQPSocketConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Message\AMQPMessage;

class ConnectionFactoryTest extends TestCase
{
    /**
     * @dataProvider transports
     */
    public function testConfiguredTransportCanPublishAndAcknowledge($type, $expectedType, $lazy, $tls)
    {
        $this->configureTransport($type, $tls);
        $connection = \Yii::$app->rabbitmq->getConnection();
        try {
            $this->assertSame($expectedType, get_class($connection));
            $this->assertSame(!$lazy, $connection->isConnected());
            $channel = $connection->channel();
            $this->assertTrue($connection->isConnected());
            [$queue] = $channel->queue_declare('', false, false, true, false);
            $channel->basic_publish(new AMQPMessage('factory test'), '', $queue);
            $received = $channel->basic_get($queue);
            $this->assertInstanceOf(AMQPMessage::class, $received);
            $this->assertSame('factory test', $received->getBody());
            $received->ack();
            $channel->close();
            $this->assertNull($connection->channel()->basic_get($queue, true));
        } finally {
            $connection->close();
        }
    }

    public static function transports(): array
    {
        return [
            'stream' => [AMQPStreamConnection::class, AMQPStreamConnection::class, false, false],
            'lazy stream' => [AMQPLazyConnection::class, AMQPLazyConnection::class, true, false],
            'socket' => [AMQPSocketConnection::class, AMQPSocketConnection::class, false, false],
            'lazy socket' => [AMQPLazySocketConnection::class, AMQPLazySocketConnection::class, true, false],
            'TLS stream' => [AMQPStreamConnection::class, AMQPStreamConnection::class, false, true],
            'custom TLS stream' => [CustomStreamConnection::class, CustomStreamConnection::class, false, true],
            'lazy TLS stream' => [AMQPLazyConnection::class, AMQPLazyConnection::class, true, true],
            'legacy TLS' => [AMQPSSLConnection::class, AMQPStreamConnection::class, false, true],
            'legacy lazy TLS' => [AMQPLazySSLConnection::class, AMQPLazyConnection::class, true, true],
        ];
    }

    public function testTlsVerifiesTheCertificateHostname()
    {
        $this->configureTransport(AMQPLazySSLConnection::class, true, ['peer_name' => 'mismatched-host.invalid']);
        $connection = \Yii::$app->rabbitmq->getConnection();
        $this->expectException(AMQPIOException::class);
        $this->expectExceptionMessageMatches('/certificate|crypto/i');
        $connection->channel();
    }

    private function configureTransport(string $type, bool $tls, array $sslOptions = []): void
    {
        $url = getenv($tls ? 'RABBITMQ_TEST_TLS_URL' : 'RABBITMQ_TEST_URL_A');
        $ca = getenv('RABBITMQ_TEST_TLS_CA');
        if (!$url || ($tls && !$ca)) {
            $this->markTestSkipped($tls ? 'Set RABBITMQ_TEST_TLS_URL and RABBITMQ_TEST_TLS_CA.' : 'Set RABBITMQ_TEST_URL_A.');
        }
        $options = [
            'url' => $url, 'type' => $type, 'keepalive' => true,
            'heartbeat' => 5, 'read_write_timeout' => 12, 'channel_rpc_timeout' => 2.5,
        ];
        if ($tls) {
            $options['ssl_context'] = $sslOptions + ['cafile' => $ca];
        }
        $this->loadExtension(['components' => ['rabbitmq' => [
            'class' => Configuration::class, 'connections' => [$options],
        ]]]);
    }
}

class CustomStreamConnection extends AMQPStreamConnection
{
}
