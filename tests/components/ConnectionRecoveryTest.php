<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\Consumer;
use mikemadisonweb\rabbitmq\components\Logger;
use mikemadisonweb\rabbitmq\components\Producer;
use mikemadisonweb\rabbitmq\components\Routing;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPHeartbeatMissedException;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

class ConnectionRecoveryTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->mockApplication(['components' => ['rabbitmq' => ['class' => Configuration::class]]]);
    }

    public function testEagerServiceClosesItsOwnChannelWithoutClosingTheSharedConnection()
    {
        $connection = $this->createMock(AbstractConnection::class);
        $channel = $this->createMock(AMQPChannel::class);
        $channel->method('getChannelId')->willReturn(1);
        $connection->channels[1] = $channel;
        $connection->method('connectOnConstruct')->willReturn(true);
        $connection->method('isConnected')->willReturn(true);
        $connection->expects($this->once())->method('channel')->willReturn($channel);
        $connection->expects($this->never())->method('close');
        $channel->expects($this->once())->method('close')->willThrowException(new AMQPConnectionClosedException());
        $producer = new Producer($connection, new Routing($connection), $this->createMock(Logger::class), false);
        unset($producer);
    }

    public function testExplicitCloseToleratesAnAlreadyBrokenConnection()
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('isConnected')->willReturn(true);
        $connection->expects($this->once())->method('close')->willThrowException(new AMQPConnectionClosedException());
        $producer = new Producer($connection, new Routing($connection), $this->createMock(Logger::class), false);
        $producer->close();
    }

    public function testPublishRecoversBeforeSendingAndRunsEventsOnlyOnce()
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('isConnected')->willReturn(true);
        $checks = 0;
        $connection->expects($this->exactly(2))->method('checkHeartBeat')->willReturnCallback(function () use (&$checks) {
            if (++$checks === 1) {
                throw new AMQPHeartbeatMissedException();
            }
        });
        $connection->expects($this->once())->method('reconnect');
        $channel = $this->createMock(AMQPChannel::class);
        $channel->method('getChannelId')->willReturn(1);
        $connection->channels[1] = $channel;
        $connection->method('channel')->willReturn($channel);
        $channel->expects($this->once())->method('basic_publish')->with($this->isInstanceOf(AMQPMessage::class), 'events');
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->once())->method('reset');
        $routing->expects($this->once())->method('declareAll');
        $producer = new Producer($connection, $routing, $this->createMock(Logger::class), true);

        $events = [];
        foreach (['before_publish', 'after_publish'] as $eventName) {
            \Yii::$app->rabbitmq->on($eventName, function ($event) use (&$events) {
                $events[] = $event->name;
            });
        }
        $producer->publish('one', 'events');
        $this->assertSame(['before_publish', 'after_publish'], $events);
    }

    public function testPublishWriteFailureIsPropagatedAndNextPublishUsesNewChannel()
    {
        $error = new AMQPConnectionClosedException('Broken pipe');
        $old = $this->createMock(AMQPChannel::class);
        $new = $this->createMock(AMQPChannel::class);
        $old->expects($this->once())->method('basic_publish')->willThrowException($error);
        $new->expects($this->once())->method('basic_publish')->with(
            $this->callback(function ($message) { return $message->getBody() === 'two'; })
        );
        $connection = $this->createMock(AbstractConnection::class);
        $old->method('getChannelId')->willReturn(1);
        $new->method('getChannelId')->willReturn(1);
        $connection->method('isConnected')->willReturn(true);
        $connection->channels[1] = $old;
        $connection->expects($this->once())->method('close')->willReturnCallback(function () use ($connection, $new) {
            $connection->channels[1] = $new;
        });
        $connection->expects($this->exactly(2))->method('channel')->willReturnCallback(function () use ($connection) {
            return $connection->channels[1];
        });
        $producer = new Producer($connection, new Routing($connection), $this->createMock(Logger::class), false);
        try {
            $producer->publish('one', 'events');
            $this->fail('Expected the original publish failure.');
        } catch (AMQPConnectionClosedException $e) {
            $this->assertSame($error, $e);
        }
        $producer->publish('two', 'events');
    }

    public function testPassiveProbeMissDoesNotRecreateServerNamedQueue()
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('isConnected')->willReturn(true);
        $connection->expects($this->never())->method('reconnect');
        $old = $this->createMock(AMQPChannel::class);
        $new = $this->createMock(AMQPChannel::class);
        $old->method('getChannelId')->willReturn(1);
        $new->method('getChannelId')->willReturn(1);
        $channels = [$old, $new];
        $connection->expects($this->exactly(2))->method('channel')->willReturnCallback(
            function () use ($connection, &$channels) {
                return $connection->channels[1] = array_shift($channels);
            }
        );
        $old->expects($this->exactly(2))->method('queue_declare')->willReturnCallback(
            function ($queue, $passive) use ($connection) {
                if ($passive) {
                    unset($connection->channels[1]);
                    throw new AMQPProtocolChannelException(404, 'Queue missing', [50, 10]);
                }
                $this->assertSame('', $queue);
            }
        );
        $new->expects($this->never())->method('queue_declare');
        $routing = new Routing($connection);
        $routing->setQueues(Configuration::DEFAULTS['queues']);
        $this->assertTrue($routing->declareAll());
        $this->assertFalse($routing->isQueueExists('missing'));
        $this->assertFalse($routing->declareAll());
    }
}
