<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\integration;

use mikemadisonweb\rabbitmq\components\Consumer;
use mikemadisonweb\rabbitmq\components\Logger;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

class ConsumerSignalTest extends TestCase
{
    public static function idleTimeouts(): array
    {
        return ['unlimited idle wait' => [0], 'positive idle timeout' => [1.0]];
    }

    /**
     * @dataProvider idleTimeouts
     * @runInSeparateProcess
     * @preserveGlobalState disabled
     */
    public function testIdleSighupRestartsWhenTheReplacementChannelHasAnotherId($idleTimeout)
    {
        $url = getenv('RABBITMQ_TEST_URL_A');
        if (!$url || !function_exists('pcntl_signal') || !function_exists('pcntl_async_signals')
            || !function_exists('pcntl_signal_get_handler') || !function_exists('posix_kill')
            || !function_exists('proc_open')) {
            $this->markTestSkipped('Requires CLI signals, subprocesses and RABBITMQ_TEST_URL_A.');
        }

        $this->loadExtension(['components' => ['rabbitmq' => [
            'class' => Configuration::class,
            'auto_declare' => false,
            'logger' => ['print_console' => false],
            'connections' => [['url' => $url, 'heartbeat' => 2, 'read_write_timeout' => 10]],
        ]]]);
        $config = \Yii::$app->rabbitmq;
        $connection = $config->getConnection();
        $queue = 'yii-idle-signal-' . bin2hex(random_bytes(8));
        // This other service's channel makes the consumer use channel 2 initially.
        $probe = $connection->channel();
        $probe->queue_declare($queue, false, true, false, false, false, new AMQPTable(['x-expires' => 15000]));
        $logger = new Logger();
        $logger->options = ['print_console' => false, 'log' => false];
        $consumer = new class($connection, $config->getRouting($connection), $logger, false) extends Consumer {
            public $afterSetup;

            protected function setup()
            {
                parent::setup();
                ($this->afterSetup)();
            }
        };
        $consumer->setIdleTimeout($idleTimeout);
        $consumer->setIdleTimeoutExitCode(75);
        $consumer->setQos(['prefetch_size' => 0, 'prefetch_count' => 1, 'global' => false]);
        $seen = [];
        $consumer->setQueues([$queue => function (AMQPMessage $message) use (&$seen, $consumer) {
            $this->assertSame($consumer->getChannel(), $message->getChannel());
            $seen[] = $message->getBody();
            return true;
        }]);
        $setups = 0;
        $originalChannelId = null;
        $process = null;
        $pipes = [];
        $consumer->afterSetup = function () use (
            &$setups, &$originalChannelId, &$process, &$pipes, $consumer, $queue
        ) {
            if (++$setups === 1) {
                $originalChannelId = $consumer->getChannel()->getChannelId();
                // A separate signal sender also bounds this test if the unlimited wait regresses.
                // Closing its stdin on completion cancels the watchdog without another signal.
                $signalSender = <<<'PHP'
stream_set_blocking(STDIN, false);
usleep(200000);
posix_kill((int)$argv[1], SIGHUP);
$deadline = microtime(true) + 5;
while (microtime(true) < $deadline) {
    fread(STDIN, 8192);
    if (feof(STDIN)) {
        exit(0);
    }
    usleep(20000);
}
fwrite(STDERR, "Consumer signal watchdog expired.\n");
posix_kill((int)$argv[1], SIGKILL);
PHP;
                $process = proc_open([
                    PHP_BINARY, '-r', $signalSender, (string)getmypid(),
                ], [['pipe', 'r'], ['pipe', 'w'], ['pipe', 'w']], $pipes);
                $this->assertIsResource($process);
            } else {
                $this->assertSame(2, $setups);
                $this->assertNotSame($originalChannelId, $consumer->getChannel()->getChannelId());
                $consumer->getChannel()->basic_publish(new AMQPMessage('after-restart'), '', $queue);
            }
        };
        $previousHandler = pcntl_signal_get_handler(SIGHUP);
        $previousAsync = pcntl_async_signals(false);
        pcntl_signal(SIGHUP, [$consumer, 'handleSignal']);

        try {
            $this->assertSame(0, $consumer->consume(1));
            $this->assertSame(2, $setups);
            $this->assertSame(['after-restart'], $seen);
            $this->assertSame(1, $consumer->getConsumed());
            $this->assertNull($connection->channel()->basic_get($queue, true));
        } finally {
            if (is_resource($process)) {
                fclose($pipes[0]);
                $stdout = stream_get_contents($pipes[1]);
                $stderr = stream_get_contents($pipes[2]);
                fclose($pipes[1]);
                fclose($pipes[2]);
                $exitCode = proc_close($process);
            }
            pcntl_signal(SIGHUP, $previousHandler);
            pcntl_async_signals($previousAsync);
            try {
                $connection->channel()->queue_delete($queue);
            } finally {
                $consumer->close();
            }
        }
        $this->assertSame(0, $exitCode);
        $this->assertSame('', $stdout . $stderr);
    }
}
