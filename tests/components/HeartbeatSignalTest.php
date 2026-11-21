<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\Consumer;
use mikemadisonweb\rabbitmq\components\ConsumerHeartbeat;
use mikemadisonweb\rabbitmq\components\Logger;
use mikemadisonweb\rabbitmq\components\Routing;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * @runTestsInSeparateProcesses
 * @preserveGlobalState disabled
 */
class HeartbeatSignalTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        if (!function_exists('pcntl_alarm') || !function_exists('pcntl_async_signals')
            || !function_exists('pcntl_signal') || !function_exists('pcntl_signal_get_handler')
            || !function_exists('pcntl_sigprocmask')) {
            $this->markTestSkipped('Requires pcntl signals.');
        }
    }

    public function testApplicationAlarmAndSignalMaskRetainOwnership()
    {
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('getHeartbeat')->willReturn(20);
        $handler = static function () {};
        pcntl_signal(SIGALRM, $handler);
        pcntl_alarm(60);
        $this->assertNull(ConsumerHeartbeat::start($connection));
        $this->assertSame($handler, pcntl_signal_get_handler(SIGALRM));
        $this->assertGreaterThan(0, pcntl_alarm(0));

        pcntl_signal(SIGALRM, SIG_IGN);
        pcntl_alarm(60);
        $this->assertNull(ConsumerHeartbeat::start($connection));
        $this->assertGreaterThan(0, pcntl_alarm(0));

        pcntl_sigprocmask(SIG_BLOCK, [SIGALRM]);
        $this->assertNull(ConsumerHeartbeat::start($connection));
        pcntl_sigprocmask(SIG_UNBLOCK, [SIGALRM], $blocked);
        $this->assertContains(SIGALRM, $blocked);
    }

    public function testSignalsCanBeDisabledForAConnectionWithHeartbeats()
    {
        define('AMQP_WITHOUT_SIGNALS', true);
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('getHeartbeat')->willReturn(20);
        $this->assertNull(ConsumerHeartbeat::start($connection));
        $this->assertSame(0, pcntl_alarm(0));
    }

    public function testHeartbeatWriteFailureAllowsTheCallbackToFinishAndPreventsAck()
    {
        $this->mockApplication(['components' => ['rabbitmq' => ['class' => Configuration::class]]]);
        $connection = $this->createMock(AbstractConnection::class);
        $connection->method('getHeartbeat')->willReturn(20);
        $connection->method('isConnected')->willReturn(true);
        $error = new AMQPConnectionClosedException('Heartbeat write failed');
        $connection->expects($this->once())->method('checkHeartBeat')->willThrowException($error);
        $consumer = $this->getMockBuilder(Consumer::class)->setConstructorArgs([
            $connection, new Routing($connection), $this->createMock(Logger::class), false,
        ])->onlyMethods(['sendResult'])->getMock();
        $consumer->expects($this->never())->method('sendResult');
        $consumer->setProceedOnException(true);
        pcntl_async_signals(false);
        $originalHandler = pcntl_signal_get_handler(SIGALRM);
        $finished = false;
        $callback = function () use (&$finished) {
            $tick = pcntl_signal_get_handler(SIGALRM);
            $tick(SIGALRM);
            $finished = true;
        };
        try {
            $this->invokeMethod($consumer, 'onReceive', [new AMQPMessage('job'), 'jobs', $callback]);
            $this->fail('Expected the heartbeat failure before ACK.');
        } catch (AMQPConnectionClosedException $e) {
            $this->assertSame($error, $e);
        }
        $this->assertTrue($finished);
        $this->assertSame(0, $consumer->getConsumed());
        $this->assertSame($originalHandler, pcntl_signal_get_handler(SIGALRM));
        $this->assertFalse(pcntl_async_signals());
        $this->assertSame(0, pcntl_alarm(0));
    }
}
