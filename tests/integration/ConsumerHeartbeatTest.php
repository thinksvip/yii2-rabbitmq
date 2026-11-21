<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\integration;

use mikemadisonweb\rabbitmq\components\ConsumerInterface;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Message\AMQPMessage;
use yii\console\ExitCode;

class ConsumerHeartbeatTest extends TestCase
{
    public function testLongCallbackAcknowledgesOnItsOriginalChannel()
    {
        $url = getenv('RABBITMQ_TEST_URL_A');
        if (!$url || !function_exists('pcntl_alarm') || !function_exists('pcntl_async_signals')
            || !function_exists('pcntl_signal_get_handler')) {
            $this->markTestSkipped('Requires pcntl alarm support and RABBITMQ_TEST_URL_A pointing to a test vhost.');
        }

        $queue = 'yii-heartbeat-' . bin2hex(random_bytes(8));
        $handler = $this->createMock(ConsumerInterface::class);
        \Yii::$container->setSingleton('heartbeat-test-handler', $handler);
        $this->loadExtension(['components' => ['rabbitmq' => [
            'class' => Configuration::class,
            'auto_declare' => false,
            'logger' => ['print_console' => false],
            'connections' => [['url' => $url, 'heartbeat' => 2, 'read_write_timeout' => 10]],
            'queues' => [['name' => $queue]],
            'consumers' => [[
                'name' => 'slow', 'callbacks' => [$queue => 'heartbeat-test-handler'], 'idle_timeout' => 5,
            ]],
        ]]]);

        $config = \Yii::$app->rabbitmq;
        $consumer = $config->getConsumer('slow');
        try {
            $channel = $consumer->getChannel();
            $connection = $config->getConnection();
            $this->assertSame(2, $connection->getHeartbeat());
            $probe = $connection->channel();
            $probe->queue_declare($queue, false, false, true, false);
            $probe->basic_publish(new AMQPMessage('slow'), '', $queue);
            $alarmHandler = pcntl_signal_get_handler(SIGALRM);
            $asyncSignals = pcntl_async_signals();
            $handler->expects($this->once())->method('execute')->willReturnCallback(function (AMQPMessage $message) use ($channel) {
                $this->assertSame($channel, $message->getChannel());
                $this->assertSame('slow', $message->getBody());
                // A signal can interrupt sleep. Keep working past the negotiated heartbeat timeout.
                $end = hrtime(true) + 7_000_000_000;
                while (hrtime(true) < $end) {
                    usleep(200000);
                }
                return ConsumerInterface::MSG_ACK;
            });
            $config->on('after_consume', function ($event) use ($channel) {
                $this->assertSame($channel, $event->consumer->getChannel());
            });

            $this->assertSame(ExitCode::OK, $consumer->consume(1));
            $this->assertFalse($channel->is_open());
            $this->assertSame($alarmHandler, pcntl_signal_get_handler(SIGALRM));
            $this->assertSame($asyncSignals, pcntl_async_signals());
            $this->assertNull($probe->basic_get($queue, true));
        } finally {
            $consumer->close();
            \Yii::$container->clear('heartbeat-test-handler');
        }
    }
}
