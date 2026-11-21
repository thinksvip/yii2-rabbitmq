<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\components;

use mikemadisonweb\rabbitmq\components\ConsumerInterface;
use mikemadisonweb\rabbitmq\components\Logger;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;
use yii\helpers\Console;
use yii\log\Logger as YiiLogger;

class LoggerTest extends TestCase
{
    public function testLogRecordsMetadataErrorsAndDisabledLogging()
    {
        $old = \Yii::getLogger();
        $sink = new YiiLogger();
        \Yii::setLogger($sink);
        try {
            $logger = new Logger();
            $logger->options = ['log' => true, 'category' => 'queue', 'print_console' => false, 'system_memory' => false];
            $this->assertSame($logger->options, $logger->getOptions());
            $msg = new AMQPMessage('payload', ['application_headers' => new AMQPTable(['trace' => 'abc'])]);
            $logger->log('delivered', $msg, [
                'timeStart' => microtime(true), 'queue' => 'q', 'exchange' => 'e',
                'routingKey' => 'r', 'processFlag' => 0, 'memory' => true,
            ]);
            $logger->logError(new \Error('handler failed'), $msg);
            [$info, $error] = $sink->messages;
            $this->assertSame('queue', $info[2]);
            $this->assertSame(YiiLogger::LEVEL_INFO, $info[1]);
            $this->assertSame('delivered', $info[0]['info']);
            $amqp = $info[0]['amqp'];
            $this->assertSame('payload', $amqp['body']);
            $this->assertSame(['trace' => 'abc'], $amqp['headers']);
            $this->assertSame('q', $amqp['extra']['queue']);
            $this->assertSame('e', $amqp['extra']['exchange']);
            $this->assertSame('r', $amqp['extra']['routing_key']);
            $this->assertSame(0, $amqp['extra']['return_code']);
            $this->assertStringContainsString('Memory usage:', $amqp['extra']['memory']);
            $this->assertStringEndsWith('s', $amqp['extra']['execution_time']);
            $this->assertSame(YiiLogger::LEVEL_ERROR, $error[1]);
            $this->assertSame('handler failed', $error[0]['msg']);
            $this->assertSame('payload', $error[0]['amqp']['message']);
            $this->assertArrayHasKey('stacktrace', $error[0]['amqp']);
            $msg->setDeliveryInfo(1, false, 'orders', 'order.created');
            $logger->log('consumed', $msg, ['queue' => 'q']);
            $delivery = $sink->messages[2][0]['amqp']['extra'];
            $this->assertSame('order.created', $delivery['routing_key']);
            $this->assertSame('orders', $delivery['exchange']);
            $logger->options['log'] = false;
            $logger->log('ignored', $msg, []);
            $logger->logError(new \Exception(), $msg);
            $this->assertCount(3, $sink->messages);
            $this->assertFalse($logger->printInfo('silent'));
        } finally {
            \Yii::setLogger($old);
        }
    }

    public function testPrintInfoWritesToStdout()
    {
        $logger = new Logger();
        $logger->options = Configuration::DEFAULTS['logger'];
        $line = '';
        if (Console::streamSupportsAnsiColors(STDOUT)) {
            $line = Console::ansiFormat($line, [Console::FG_YELLOW]);
        }
        $this->assertSame(strlen($line . PHP_EOL), $logger->printInfo(''));
    }

    /** @dataProvider dispositions */
    public function testConsoleResult($flag, $expected, $color)
    {
        $logger = $this->getMockBuilder(Logger::class)->onlyMethods(['printInfo'])->getMock();
        $logger->options = Configuration::DEFAULTS['logger'];
        $logger->options['system_memory'] = true;
        $logger->expects($this->once())->method('printInfo')->with(
            $this->callback(function ($message) use ($expected) {
                $this->assertStringContainsString('queue `jobs`', $message);
                $this->assertStringContainsString($expected, $message);
                $this->assertStringContainsString(is_readable('/proc/meminfo') ? 'MemTotal:' : 'Memory usage:', $message);
                return true;
            }), $color
        );
        $logger->printResult('jobs', $flag, microtime(true));
    }

    public static function dispositions(): array
    {
        return [
            [ConsumerInterface::MSG_ACK, 'consumed successfully', Console::FG_GREEN],
            [ConsumerInterface::MSG_REJECT, 'dropped from queue', Console::FG_RED],
            [ConsumerInterface::MSG_REQUEUE, 'sent back to queue', Console::FG_RED],
            [false, 'sent back to queue', Console::FG_RED],
        ];
    }

    public function testConsoleErrorAndDisabledOutput()
    {
        $logger = $this->getMockBuilder(Logger::class)->onlyMethods(['printInfo'])->getMock();
        $logger->options = Configuration::DEFAULTS['logger'];
        $logger->expects($this->once())->method('printInfo')->with($this->stringContains('Error: failed'), Console::FG_RED);
        $error = new \Error('failed');
        $logger->printError($error);
        $logger->options['print_console'] = false;
        $logger->printError($error);
        $logger->printResult('jobs', 0, microtime(true));
    }
}
