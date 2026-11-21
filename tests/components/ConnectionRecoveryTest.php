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

    public static function settlementFailures(): array
    {
        return [
            'broken connection' => [new AMQPConnectionClosedException('Connection closed before ack')],
            'ack timed out' => [new AMQPTimeoutException('Write timed out')],
        ];
    }

    /** @dataProvider settlementFailures */
    public function testConsumerRestoresSubscriptionsAndQosAfterFailedAck($error)
    {
        $old = $this->createMock(AMQPChannel::class);
        $new = $this->createMock(AMQPChannel::class);
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('isConnected')->willReturn(true);
        $connection->channels[1] = $old;
        $connection->method('channel')->willReturnCallback(function () use ($connection) {
            return $connection->channels[1];
        });
        $connection->expects($this->once())->method('reconnect')->willReturnCallback(function () use ($connection, $new) {
            $connection->channels[1] = $new;
        });
        foreach ([$old, $new] as $channel) {
            $channel->method('getChannelId')->willReturn(1);
            $channel->expects($this->once())->method('basic_qos')->with(0, 1, false);
            $channel->expects($this->once())->method('basic_consume')->willReturnCallback(
                function ($queue, $tag, $noLocal, $noAck, $exclusive, $nowait, $callback) use ($channel) {
                    $channel->callbacks[$tag] = $callback;
                }
            );
            $channel->method('wait')->willReturnCallback(function () use ($channel) {
                $message = new AMQPMessage('job');
                $message->setChannel($channel)->setDeliveryInfo(1, false, '', 'jobs');
                reset($channel->callbacks)($message);
            });
        }
        $acks = 0;
        $old->expects($this->exactly(2))->method('basic_ack')->willReturnCallback(function () use (&$acks, $error) {
            if (++$acks === 2) {
                throw $error;
            }
        });
        $new->expects($this->once())->method('basic_ack');
        $new->expects($this->once())->method('basic_cancel');
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->exactly(2))->method('declareAll');
        $consumer = new Consumer($connection, $routing, $this->createMock(Logger::class), true);
        $consumer->setQos(['prefetch_size' => 0, 'prefetch_count' => 1, 'global' => false]);
        $consumer->setQueues(['jobs' => function () { return true; }]);
        $consumer->setProceedOnException(true);
        $consumer->setIdleTimeoutExitCode(22);
        $this->assertSame(0, $consumer->consume(2));
        $this->assertSame(2, $consumer->getConsumed());
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

    public function testConsumerAttemptsOneReconnectAfterSetupTimeout()
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->expects($this->once())->method('reconnect');
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->exactly(2))->method('declareAll')->willThrowException(new AMQPTimeoutException());
        $consumer = new Consumer($connection, $routing, $this->createMock(Logger::class), true);
        $consumer->setIdleTimeoutExitCode(22);
        $this->expectException(AMQPTimeoutException::class);
        $consumer->consume();
    }

    public function testApplicationTransportExceptionIsNotRetried()
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->expects($this->never())->method('reconnect');
        $channel = $this->createMock(AMQPChannel::class);
        $connection->method('channel')->willReturn($channel);
        $channel->method('basic_consume')->willReturnCallback(
            function ($queue, $tag, $noLocal, $noAck, $exclusive, $nowait, $callback) use ($channel) {
                $channel->callbacks[$tag] = $callback;
            }
        );
        $channel->expects($this->once())->method('wait')->willReturnCallback(function () use ($channel) {
            reset($channel->callbacks)(new AMQPMessage('job'));
        });
        $consumer = new Consumer($connection, new Routing($connection), $this->createMock(Logger::class), false);
        $consumer->setQueues(['jobs' => function () { throw new AMQPConnectionClosedException('Publisher lost'); }]);
        $this->expectException(AMQPConnectionClosedException::class);
        $consumer->consume();
    }

    public function testIdleTimeoutKeepsConfiguredExitCode()
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->expects($this->never())->method('reconnect');
        $channel = $this->createMock(AMQPChannel::class);
        $channel->callbacks = ['jobs' => function () {}];
        $channel->expects($this->once())->method('wait')->willThrowException(new AMQPTimeoutException());
        $connection->method('channel')->willReturn($channel);
        $consumer = new Consumer($connection, new Routing($connection), $this->createMock(Logger::class), false);
        $consumer->setIdleTimeout(1);
        $consumer->setIdleTimeoutExitCode(22);
        $this->assertSame(22, $consumer->consume());
    }
}
