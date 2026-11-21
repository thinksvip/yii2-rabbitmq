<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests;

use mikemadisonweb\rabbitmq\components\ConsumerInterface;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\exceptions\InvalidConfigException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;

class VhostRoutingTest extends TestCase
{
    public function testExchangeBindingsRespectBothVhostsAndAllowExternalDestinations()
    {
        $config = (new Configuration([
            'connections' => [['name' => 'a', 'host' => 'localhost'], ['name' => 'b', 'host' => 'localhost']],
            'exchanges' => [
                ['name' => 'events', 'type' => 'fanout'],
                ['name' => 'internal-destination', 'type' => 'topic', 'connection' => 'a'],
            ],
            'bindings' => [
                ['exchange' => 'events', 'to_exchange' => 'internal-destination'],
                ['exchange' => 'events', 'to_exchange' => 'external-destination'],
            ],
        ]))->getConfig();
        $this->assertSame(['internal-destination', 'external-destination'],
            array_column($config->getRoutingConfiguration('a')['bindings'], 'to_exchange'));
        $this->assertSame(['external-destination'], array_column($config->getRoutingConfiguration('b')['bindings'], 'to_exchange'));
    }

    public function testAutoDeclarationUsesTheSelectedConnection()
    {
        $this->loadExtension(['components' => ['rabbitmq' => [
            'class' => Configuration::class,
            'connections' => [
                ['name' => 'a', 'host' => 'localhost', 'vhost' => '/a'],
                ['name' => 'b', 'host' => 'localhost', 'vhost' => '/b'],
            ],
            'queues' => [
                ['name' => 'common'],
                ['name' => 'jobs', 'connection' => 'a', 'durable' => false],
                ['name' => 'jobs', 'connection' => 'b'],
                ['name' => 'a-only', 'connection' => 'a'],
            ],
            'exchanges' => [['name' => 'events', 'type' => 'direct']],
            'bindings' => [
                ['exchange' => 'events', 'queue' => 'a-only'],
                ['exchange' => 'events', 'queue' => 'jobs', 'connection' => 'b'],
            ],
            'producers' => [
                ['name' => 'pa', 'connection' => 'a', 'safe' => false],
                ['name' => 'pb', 'connection' => 'b', 'safe' => false],
            ],
            'consumers' => [
                ['name' => 'ca', 'connection' => 'a', 'callbacks' => ['jobs' => 'test-callback']],
            ],
        ]]]);
        \Yii::$container->setSingleton('test-callback', $this->createMock(ConsumerInterface::class));
        $declared = [];
        foreach (['a' => 'a-only', 'b' => 'jobs'] as $name => $boundQueue) {
            $channel = $this->createMock(AMQPChannel::class);
            $channel->method('getChannelId')->willReturn(1);
            $channel->method('is_open')->willReturn(true);
            $channel->method('queue_declare')->willReturnCallback(function ($queue, $passive, $durable) use (&$declared, $name) {
                $declared[$name][$queue] = $durable;
            });
            $channel->expects($this->once())->method('exchange_declare')->with('events');
            $channel->expects($this->once())->method('queue_bind')->with($boundQueue, 'events');
            $channel->expects($this->once())->method('basic_publish');
            $connection = $this->createMock(AbstractConnection::class);
            $connection->channels[1] = $channel;
            $connection->method('channel')->willReturn($channel);
            $connection->method('isConnected')->willReturn(true);
            \Yii::$container->setSingleton(sprintf(Configuration::CONNECTION_SERVICE_NAME, $name), $connection);
        }

        $config = \Yii::$app->rabbitmq;
        $config->getProducer('pb')->publish('first', 'events');
        $this->assertArrayNotHasKey('a', $declared);
        $this->assertFalse(\Yii::$container->hasSingleton(sprintf(Configuration::CONNECTION_SERVICE_NAME, 'a'), true));
        $config->getProducer('pa')->publish('second', 'events');
        $this->assertSame(['common' => true, 'jobs' => false, 'a-only' => true], $declared['a']);
        $this->assertSame(['common' => true, 'jobs' => true], $declared['b']);
        $routingA = $config->getRouting($config->getConnection('a'));
        $this->assertSame($routingA, $this->getInaccessibleProperty($config->getConsumer('ca'), 'routing'));
        $this->assertNotSame($routingA, $config->getRouting($config->getConnection('b')));
        $this->assertFalse($routingA->declareAll());
    }

    public function testRejectsBindingBetweenDifferentVhosts()
    {
        $this->expectException(InvalidConfigException::class);
        $config = new Configuration([
            'connections' => [
                ['name' => 'a', 'host' => 'localhost', 'vhost' => '/a'],
                ['name' => 'b', 'host' => 'localhost', 'vhost' => '/b'],
            ],
            'exchanges' => [['name' => 'events', 'type' => 'direct', 'connection' => 'a']],
            'queues' => [['name' => 'jobs', 'connection' => 'b']],
            'bindings' => [['exchange' => 'events', 'queue' => 'jobs']],
        ]);
        $config->getConfig();
    }
}
