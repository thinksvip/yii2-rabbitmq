<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\Routing;
use mikemadisonweb\rabbitmq\exceptions\RuntimeException;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;

class RoutingTest extends TestCase
{
    public function testExclusiveRecoveryWaitsOnlyForAPreviousOwnerAndStopsRetrying()
    {
        $connection = $this->createMock(AMQPLazyConnection::class);
        $connection->method('isConnected')->willReturn(true);
        $attempts = 0;
        $locked = 0;
        $lost = new AMQPProtocolChannelException(405, 'Resource locked', [50, 10]);
        $connection->method('channel')->willReturnCallback(function () use ($connection, &$attempts, &$locked, $lost) {
            $channel = $this->createMock(AMQPChannel::class);
            $channel->method('getChannelId')->willReturn(1);
            $channel->method('queue_declare')->willReturnCallback(function () use (&$attempts, &$locked, $lost) {
                $attempts++;
                if ($locked > 0) {
                    $locked--;
                    throw $lost;
                }
                return ['jobs', 0, 0];
            });
            return $connection->channels[1] = $channel;
        });
        $routing = new Routing($connection);
        $routing->setQueues([array_replace(Configuration::DEFAULTS['queues'][0], ['name' => 'jobs', 'exclusive' => true])]);
        $routing->declareQueue('jobs');
        $routing->declareQueue('jobs');
        $this->assertSame(1, $attempts);
        $routing->reset();
        $locked = 1;
        $routing->declareQueue('jobs');
        $this->assertSame(3, $attempts);
        $routing->reset();
        $locked = 100;
        $before = $attempts;
        try {
            $routing->declareQueue('jobs');
            $this->fail('Expected the exclusive queue to remain locked.');
        } catch (AMQPProtocolChannelException $e) {
            $this->assertSame($lost, $e);
        }
        $this->assertGreaterThan(1, $attempts - $before);
        $this->assertLessThan($locked, $attempts - $before);

        $before = $attempts;
        try {
            $routing->declareQueue('jobs');
            $this->fail('Expected the lock held by the new owner.');
        } catch (AMQPProtocolChannelException $e) {
            $this->assertSame($lost, $e);
        }
        $this->assertSame(1, $attempts - $before);
    }

    public function testRouting()
    {
        $name = 'test';
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'name' => $name,
                            'url' => 'amqp://user:pass@host:5432/vhost?connection_timeout=3',
                        ],
                    ],
                    'exchanges' => [
                        [
                            'name' => $name,
                            'type' => 'direct'
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $name,
                            'durable' => true,
                        ],
                        [
                            'durable' => false,
                        ],
                    ],
                    'bindings' => [
                        [
                            'queue' => $name,
                            'exchange' => $name,
                            'routing_keys' => [$name],
                        ],
                        [
                            'exchange' => $name,
                            'to_exchange' => $name,
                            'routing_keys' => [$name],
                        ],
                        [
                            'queue' => $name,
                            'exchange' => $name,
                        ],
                        [
                            'exchange' => $name,
                            'to_exchange' => $name,
                        ],
                        [
                            'queue' => '',
                            'exchange' => $name,
                        ],
                    ],
                ],
            ],
        ]);
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['channel', 'isConnected'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $connection->method('isConnected')->willReturn(true);
        $channel->method('getChannelId')->willReturn(1);
        $channel->method('queue_declare')->willReturn(['amq.gen-fixture', 0, 0]);
        $connection->channels[1] = $channel;
        $connection->method('channel')
            ->willReturn($channel);
        $routing = \Yii::$app->rabbitmq->getRouting($connection);
        $this->assertTrue($routing->declareAll());
        $this->assertFalse($routing->declareAll());
    }

    /**
     * @dataProvider checkExceptions
     * @param $functionName
     */
    public function testRoutingExceptions($functionName)
    {
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'url' => 'amqp://user:pass@host:5432/vhost?connection_timeout=3',
                        ],
                    ],
                ],
            ],
        ]);
        $connection = \Yii::$app->rabbitmq->getConnection();
        $routing = \Yii::$app->rabbitmq->getRouting($connection);
        $this->expectException(RuntimeException::class);
        $routing->$functionName('non-existing');
    }

    /**
     * @return array
     */
    public function checkExceptions() : array
    {
        return [
            ['declareQueue'],
            ['declareExchange'],
            ['purgeQueue'],
            ['deleteQueue'],
            ['deleteExchange'],
        ];
    }

    public function testRoutingNonExisting()
    {
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'url' => 'amqp://user:pass@host:5432/vhost?connection_timeout=3',
                        ],
                    ],
                ],
            ],
        ]);
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $exception = new AMQPProtocolChannelException(404, 'Not found', [50, 10]);
        $channel
            ->expects($this->once())
            ->method('exchange_declare')
            ->willThrowException($exception);
        $channel
            ->expects($this->once())
            ->method('queue_declare')
            ->willThrowException($exception);
        $connection->method('channel')
            ->willReturn($channel);
        $routing = \Yii::$app->rabbitmq->getRouting($connection);
        $this->assertFalse($routing->isExchangeExists('non-existing'));
        $this->assertFalse($routing->isQueueExists('non-existing'));
    }

    public function testExchangeProbePreservesBrokerPermissionErrors()
    {
        $connection = $this->createMock(AMQPLazyConnection::class);
        $channel = $this->createMock(AMQPChannel::class);
        $connection->method('channel')->willReturn($channel);
        $denied = new AMQPProtocolChannelException(403, 'Access refused', [40, 10]);
        $channel->expects($this->once())->method('exchange_declare')->willThrowException($denied);
        $routing = new Routing($connection);
        $this->expectException(AMQPProtocolChannelException::class);
        $this->expectExceptionCode(403);
        $routing->isExchangeExists('restricted');
    }

    public function testRoutingExisting()
    {
        $name = 'test';
        $this->loadExtension([
            'components' => [
                'rabbitmq' => [
                    'class' => Configuration::class,
                    'connections' => [
                        [
                            'url' => 'amqp://user:pass@host:5432/vhost?connection_timeout=3',
                        ],
                    ],
                    'exchanges' => [
                        [
                            'name' => $name,
                            'type' => 'direct'
                        ],
                    ],
                    'queues' => [
                        [
                            'name' => $name,
                            'durable' => true,
                        ],
                        [
                            'durable' => false,
                        ],
                    ],
                ],
            ],
        ]);
        $connection = $this->getMockBuilder(AMQPLazyConnection::class)
            ->disableOriginalConstructor()
            ->setMethods(['channel'])
            ->getMock();
        $channel = $this->getMockBuilder(AMQPChannel::class)
            ->disableOriginalConstructor()
            ->getMock();
        $channel
            ->expects($this->once())
            ->method('exchange_declare');
        $channel
            ->expects($this->once())
            ->method('queue_declare');
        $channel
            ->expects($this->once())
            ->method('queue_purge');
        $connection->method('channel')
            ->willReturn($channel);
        $routing = \Yii::$app->rabbitmq->getRouting($connection);
        $this->assertTrue($routing->isExchangeExists($name));
        $this->assertTrue($routing->isQueueExists($name));
        // Test purging queue
        $routing->purgeQueue($name);
        // Test deleting all schema
        $channel
            ->expects($this->once())
            ->method('queue_delete')->with($name);
        $channel
            ->expects($this->once())
            ->method('exchange_delete');
        $routing->deleteAll();
    }
}
