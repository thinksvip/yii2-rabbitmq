<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\Consumer;
use mikemadisonweb\rabbitmq\components\Logger;
use mikemadisonweb\rabbitmq\components\Routing;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

/** @requires extension pcntl */
class ConsumerSignalTest extends TestCase
{
    private $consumer;
    private $channel;
    private $connection;
    private $waitCallback;
    private $actions = [];

    protected function setUp(): void
    {
        parent::setUp();
        $this->mockApplication(['components' => ['rabbitmq' => ['class' => Configuration::class]]]);
        $this->channel = $this->createMock(AMQPChannel::class);
        $this->connection = $this->createMock(AbstractConnection::class);
        $this->connection->method('isConnected')->willReturn(true);
        $this->connection->method('channel')->willReturn($this->channel);
        $this->connection->channels[1] = $this->channel;
        $this->channel->method('getChannelId')->willReturn(1);
        $this->channel->method('basic_cancel')->willReturnCallback(function ($tag) {
            $this->actions[] = 'cancel';
            unset($this->channel->callbacks[$tag]);
        });
        $this->channel->method('basic_consume')->willReturnCallback(
            function ($queue, $tag, $noLocal, $noAck, $exclusive, $nowait, $callback) {
                $this->channel->callbacks[$tag] = $callback;
            }
        );
        $this->waitCallback = function () {
            $message = (new AMQPMessage('job'))->setChannel($this->channel)->setDeliveryInfo(1, false, '', 'jobs');
            reset($this->channel->callbacks)($message);
        };
        $this->channel->method('wait')->willReturnCallback(function ($allowed, $nonBlocking, $timeout) {
            $this->assertSame($this->consumer->getIdleTimeout(), $timeout);
            ($this->waitCallback)();
        });
        $this->consumer = new Consumer($this->connection, new Routing($this->connection), $this->createMock(Logger::class), false);
    }

    public function testStopOverridesRestartUntilAckHasCompleted()
    {
        $this->consumer->setQueues(['jobs' => function () {
            $this->consumer->handleSignal(SIGHUP);
            $this->consumer->handleSignal(SIGTERM);
            $this->actions[] = 'handled';
        }]);
        $this->channel->expects($this->once())->method('basic_ack')->willReturnCallback(function () {
            $this->consumer->handleSignal(SIGHUP);
            $this->actions[] = 'acked';
        });
        $this->connection->expects($this->never())->method('reconnect');
        $this->assertSame(0, $this->consumer->consume());
        $this->assertSame(['handled', 'acked', 'cancel'], $this->actions);
    }

    public function testRestartRunsAfterAck()
    {
        $this->consumer->setQueues(['jobs' => function () { $this->consumer->handleSignal(SIGHUP); }]);
        $this->channel->expects($this->once())->method('basic_ack')->willReturnCallback(function () {
            $this->actions[] = 'acked';
        });
        $this->connection->expects($this->once())->method('reconnect')->willReturnCallback(function () {
            $this->actions[] = 'reconnect';
        });
        $this->assertSame(0, $this->consumer->consume(1));
        // The second cancel is the message limit stopping the replacement subscription.
        $this->assertSame(['acked', 'cancel', 'reconnect', 'cancel'], $this->actions);
    }

    public function testStopDuringFailedAckDoesNotReconnect()
    {
        $this->consumer->setQueues(['jobs' => function () { return true; }]);
        $this->channel->expects($this->once())->method('basic_ack')->willReturnCallback(function () {
            $this->consumer->handleSignal(SIGINT);
            throw new AMQPConnectionClosedException();
        });
        $this->connection->expects($this->never())->method('reconnect');
        $this->connection->expects($this->never())->method('close');
        $this->channel->expects($this->once())->method('close');
        $this->assertSame(0, $this->consumer->consume());
        $this->assertSame(0, $this->consumer->getConsumed());
        $this->assertSame([], $this->actions);
    }

    public function testPendingRestartAndFailedAckUseOneReconnect()
    {
        $this->consumer->setQueues(['jobs' => function () { return true; }]);
        $acks = 0;
        $this->channel->expects($this->exactly(2))->method('basic_ack')->willReturnCallback(function () use (&$acks) {
            if (++$acks === 1) {
                $this->consumer->handleSignal(SIGHUP);
                throw new AMQPConnectionClosedException();
            }
        });
        $this->connection->expects($this->once())->method('reconnect')->willReturnCallback(function () {
            $this->channel->callbacks = [];
            $this->actions[] = 'reconnect';
        });
        $this->assertSame(0, $this->consumer->consume(1));
        $this->assertSame(['reconnect', 'cancel'], $this->actions);
    }

    public function testHandlerExceptionTakesPrecedenceOverPendingRestart()
    {
        $error = new \RuntimeException('Handler failed');
        $this->consumer->setQueues(['jobs' => function () use ($error) {
            $this->consumer->handleSignal(SIGHUP);
            throw $error;
        }]);
        $this->connection->expects($this->never())->method('reconnect');
        $this->channel->expects($this->never())->method('basic_ack');
        try {
            $this->consumer->consume();
            $this->fail('Expected the original exception from the handler.');
        } catch (\RuntimeException $caught) {
            $this->assertSame($error, $caught);
        }
        $this->assertSame([], $this->actions);
        $this->consumer->handleSignal(SIGTERM);
        $this->assertSame([], $this->actions);
        $this->assertSame(0, $this->consumer->consume());
        $this->assertSame(['cancel'], $this->actions);
    }

    public function testIdleRestartWaitsUntilTheActiveWaitHasUnwound()
    {
        $insideWait = false;
        $deliver = $this->waitCallback;
        $waits = 0;
        $this->waitCallback = function () use (&$insideWait, &$waits, $deliver) {
            if (++$waits === 1) {
                $insideWait = true;
                $this->consumer->handleSignal(SIGHUP);
                $this->assertSame([], $this->actions);
                $insideWait = false;
                throw new AMQPTimeoutException('Idle wait expired');
            }
            $deliver();
        };
        $this->connection->expects($this->once())->method('reconnect')->willReturnCallback(function () use (&$insideWait) {
            $this->assertFalse($insideWait);
            $this->actions[] = 'reconnect';
        });
        $this->consumer->setQueues(['jobs' => function () { return true; }]);
        $this->assertSame(0, $this->consumer->consume(1));
        $this->assertSame(2, $waits);
        $this->assertSame(['cancel', 'reconnect', 'cancel'], $this->actions);
    }

    public function testIdleStopDoesNotBecomeAnIdleTimeoutOrReconnect()
    {
        $this->waitCallback = function () {
            $this->consumer->handleSignal(SIGTERM);
            $this->assertSame([], $this->actions);
            throw new AMQPTimeoutException('Idle wait expired');
        };
        $this->consumer->setQueues(['jobs' => function () { $this->fail('No delivery was received.'); }]);
        $this->consumer->setIdleTimeoutExitCode(75);
        $this->connection->expects($this->never())->method('reconnect');
        $this->assertSame(0, $this->consumer->consume());
        $this->assertSame(0, $this->consumer->getConsumed());
        $this->assertSame(['cancel'], $this->actions);
    }

    public function testLongCallbacksDoNotUseTheNextDeliveriesIdleBudget()
    {
        $this->consumer->setIdleTimeout(0.01);
        $this->consumer->setIdleTimeoutExitCode(75);
        $this->consumer->setQueues(['jobs' => static function () {
            usleep(20000);
            return true;
        }]);
        $this->assertSame(0, $this->consumer->consume(2));
        $this->assertSame(2, $this->consumer->getConsumed());
    }

}
