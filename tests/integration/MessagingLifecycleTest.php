<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\integration;

use mikemadisonweb\rabbitmq\components\ConsumerInterface;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;

class MessagingLifecycleTest extends TestCase
{
    private $prefix;
    private $url;

    protected function setUp(): void
    {
        parent::setUp();
        $this->url = getenv('RABBITMQ_TEST_URL_A');
        if (!$this->url) {
            $this->markTestSkipped('Requires RABBITMQ_TEST_URL_A pointing to a test vhost.');
        }
        $this->prefix = 'yii-lifecycle-' . bin2hex(random_bytes(8));
    }

    private function configure(array $options): Configuration
    {
        $this->loadExtension(['components' => ['rabbitmq' => $options + [
            'class' => Configuration::class, 'connections' => [['url' => $this->url]],
            'logger' => ['print_console' => false],
        ]]]);

        return \Yii::$app->rabbitmq;
    }

    public function testPublishConsumeSettlementAndDeadLetterRouting()
    {
        $queue = $this->prefix . '-jobs';
        $dead = $this->prefix . '-dead';
        $exchange = $this->prefix . '-events';
        $handler = $this->createMock(ConsumerInterface::class);
        \Yii::$container->setSingleton('lifecycle-handler', $handler);
        $config = $this->configure([
            'exchanges' => [['name' => $exchange, 'type' => 'direct', 'auto_delete' => true, 'durable' => false]],
            'queues' => [
                ['name' => $queue, 'exclusive' => true, 'durable' => false, 'arguments' => [
                    'x-dead-letter-exchange' => ['S', ''], 'x-dead-letter-routing-key' => ['S', $dead],
                ]],
                ['name' => $dead, 'exclusive' => true, 'durable' => false],
            ],
            'bindings' => [['queue' => $queue, 'exchange' => $exchange, 'routing_keys' => ['work']]],
            'producers' => [['name' => 'sender', 'serializer' => 'json_encode']],
            'consumers' => [[
                'name' => 'worker', 'callbacks' => [$queue => 'lifecycle-handler'],
                'qos' => ['prefetch_count' => 1], 'idle_timeout' => 3,
                'deserializer' => static function ($body) { return json_decode($body, true); },
            ]],
        ]);
        $events = [];
        foreach (['before_publish', 'after_publish', 'before_consume', 'after_consume'] as $eventName) {
            $config->on($eventName, static function ($event) use (&$events) {
                $id = $event->message->get('message_id');
                $events[$id][$event->name][] = $event->message->getBody();
            });
        }
        $producer = $config->getProducer('sender');
        $publisherChannel = $producer->getChannel();
        $publisherChannel->confirm_select();
        $consumer = $config->getConsumer('worker');
        $delivery = [];

        try {
            foreach (['ack', 'reject', 'requeue', 'false'] as $id) {
                $producer->publish(['id' => $id], $exchange, 'work', [
                    'message_id' => $id, 'content_type' => 'application/json', 'delivery_mode' => 1,
                ], ['attempt' => 42]);
            }
            $producer->publish('raw', $exchange, 'work', ['message_id' => 'raw']);
            $producer->publish('ignored', $exchange, 'other', ['message_id' => 'unrouted']);
            $publisherChannel->wait_for_pending_acks(3);
            $probe = $config->getConnection()->channel();
            $handler->expects($this->exactly(7))->method('execute')->willReturnCallback(
                function (AMQPMessage $msg) use (&$delivery, $probe, $queue, $exchange) {
                    if (!$delivery) {
                        // With prefetch=1, four of the five routable messages must still be ready.
                        $this->assertSame(4, $probe->queue_declare($queue, true)[1]);
                    }
                    $id = $msg->get('message_id');
                    $delivery[$id][] = $msg->isRedelivered();
                    $this->assertSame('work', $msg->getRoutingKey());
                    $this->assertSame($exchange, $msg->getExchange());
                    if ($id === 'raw') {
                        $this->assertSame('raw', $msg->getBody());
                        $this->assertFalse($msg->has('application_headers'));
                    } else {
                        $this->assertSame(['id' => $id], $msg->getBody());
                        $this->assertSame('application/json', $msg->get('content_type'));
                        $this->assertSame(1, $msg->get('delivery_mode'));
                        $this->assertSame(['attempt' => 42, 'rabbitmq.serialized' => 1], $msg->get('application_headers')->getNativeData());
                    }
                    if ($id === 'reject') {
                        return ConsumerInterface::MSG_REJECT;
                    }
                    if (!$msg->isRedelivered() && $id === 'requeue') {
                        return ConsumerInterface::MSG_REQUEUE;
                    }
                    if (!$msg->isRedelivered() && $id === 'false') {
                        return false;
                    }
                    return ConsumerInterface::MSG_ACK;
                }
            );
            $this->assertSame(0, $consumer->consume(7));
            $this->assertSame(7, $consumer->getConsumed());
            foreach (['ack', 'reject', 'raw'] as $id) {
                $this->assertSame([false], $delivery[$id]);
            }
            foreach (['requeue', 'false'] as $id) {
                $this->assertSame([false, true], $delivery[$id]);
            }
            $this->assertSame(['before_publish', 'after_publish'], array_keys($events['unrouted']));
            $json = '{"id":"ack"}';
            $this->assertSame([
                'before_publish' => [$json], 'after_publish' => [$json],
                'before_consume' => [$json], 'after_consume' => [['id' => 'ack']],
            ], $events['ack']);
            $this->assertSame('reject', $probe->basic_get($dead, true)->get('message_id'));
            // consume() has released its delivery channel, making any unacked delivery visible.
            $this->assertNull($probe->basic_get($queue, true));
            $this->assertNull($probe->basic_get($dead, true));
        } finally {
            $consumer->close();
            \Yii::$container->clear('lifecycle-handler');
        }
    }

    public function testProceedOnExceptionRequeuesTheDeliveryAndReleasesPrefetch()
    {
        $queue = $this->prefix . '-retry';
        $handler = $this->createMock(ConsumerInterface::class);
        \Yii::$container->setSingleton('retry-handler', $handler);
        $config = $this->configure([
            'queues' => [['name' => $queue, 'durable' => false, 'exclusive' => true]],
            'producers' => [['name' => 'sender']],
            'consumers' => [[
                'name' => 'worker', 'callbacks' => [$queue => 'retry-handler'],
                'qos' => ['prefetch_count' => 1], 'proceed_on_exception' => true,
                'idle_timeout' => 2, 'idle_timeout_exit_code' => 75,
            ]],
        ]);
        $seen = [];
        $handler->method('execute')->willReturnCallback(static function (AMQPMessage $message) use (&$seen) {
            $body = $message->getBody();
            $seen[$body][] = $message->isRedelivered();
            if ($body === 'retry' && !$message->isRedelivered()) {
                throw new \RuntimeException('Temporary failure in handler.');
            }
            if ($body === 'manual') {
                $message->ack();
                throw new \RuntimeException('Handler failed after acknowledging its delivery.');
            }
            return ConsumerInterface::MSG_ACK;
        });
        $after = [];
        $config->on('after_consume', static function ($event) use (&$after) {
            $after[] = $event->message->getBody();
            throw new \RuntimeException('Listener failed after acknowledgement.');
        });
        $connection = $config->getConnection();
        try {
            $producer = $config->getProducer('sender');
            $producer->publish('retry', '', $queue);
            $producer->publish('manual', '', $queue);
            $producer->publish('next', '', $queue);
            $consumer = $config->getConsumer('worker');
            $this->assertSame(0, $consumer->consume(4));
            $this->assertSame([false, true], $seen['retry']);
            $this->assertSame([false], $seen['manual']);
            $this->assertSame([false], $seen['next']);
            sort($after);
            $this->assertSame(['next', 'retry'], $after);
            $this->assertNull($consumer->getChannel()->basic_get($queue, true));
        } finally {
            $connection->close();
        }
    }

    public function testTopologyCanBePurgedDeletedAndDeclaredAgain()
    {
        $queue = $this->prefix . '-jobs';
        $source = $this->prefix . '-source';
        $target = $this->prefix . '-target';
        $config = $this->configure([
            'auto_declare' => false,
            'exchanges' => [
                ['name' => $source, 'type' => 'topic', 'durable' => false],
                ['name' => $target, 'type' => 'fanout', 'durable' => false],
            ],
            'queues' => [['name' => $queue, 'exclusive' => true, 'durable' => false]],
            'bindings' => [
                ['exchange' => $source, 'to_exchange' => $target, 'routing_keys' => ['order.*', 'billing.#']],
                ['exchange' => $target, 'queue' => $queue],
            ],
            'producers' => [['name' => 'sender']],
        ]);
        $connection = $config->getConnection();
        $routing = $config->getRouting($connection);
        $producer = $config->getProducer('sender');
        $publisherChannel = $producer->getChannel();
        $publisherChannel->confirm_select();
        try {
            $this->assertFalse($routing->isQueueExists($queue));
            $this->assertFalse($routing->isExchangeExists($source));
            $this->assertTrue($routing->declareAll());
            $this->assertFalse($routing->declareAll());
            $probe = $connection->channel();
            $producer->publish('order', $source, 'order.new');
            $producer->publish('billing', $source, 'billing.card.paid');
            $producer->publish('unbound', $source, 'order.card.paid');
            $publisherChannel->wait_for_pending_acks(3);
            $this->assertSame(2, $probe->queue_declare($queue, true)[1]);
            $routing->purgeQueue($queue);
            $this->assertTrue($routing->isQueueExists($queue));
            $this->assertNull($probe->basic_get($queue, true));

            $routing->deleteAll();
            $this->assertFalse($routing->isQueueExists($queue));
            $this->assertFalse($routing->isExchangeExists($target));
            $this->assertTrue($routing->declareAll());
            $producer->publish('recreated', $source, 'order.new');
            $publisherChannel->wait_for_pending_acks(3);
            $this->assertSame('recreated', $probe->basic_get($queue, true)->getBody());
        } finally {
            foreach ([$source, $target] as $name) {
                if ($routing->isExchangeExists($name)) {
                    $routing->deleteExchange($name);
                }
            }
            $connection->close();
        }
    }

    public function testServerNamedAndNumericQueuesUseTheirOwnBindingsAndCallbacks()
    {
        $number = (string)random_int(100000000, 999999999);
        $exchange = $this->prefix . '-headers';
        $anonymous = $this->createMock(ConsumerInterface::class);
        $numeric = $this->createMock(ConsumerInterface::class);
        \Yii::$container->setSingleton('anonymous-handler', $anonymous);
        \Yii::$container->setSingleton('numeric-handler', $numeric);
        $config = $this->configure([
            'exchanges' => [['name' => $exchange, 'type' => 'headers', 'durable' => false, 'auto_delete' => true]],
            'queues' => [
                ['name' => '', 'durable' => false, 'exclusive' => true],
                ['name' => $number, 'durable' => false, 'exclusive' => true],
            ],
            'bindings' => [['exchange' => $exchange, 'queue' => '']],
            'producers' => [['name' => 'sender']],
            'consumers' => [[
                'name' => 'worker', 'idle_timeout' => 2,
                'callbacks' => ['' => 'anonymous-handler', $number => 'numeric-handler'],
            ]],
        ]);
        $connection = $config->getConnection();
        try {
            $producer = $config->getProducer('sender');
            $producer->publish('through default exchange', '', $number);
            $producer->publish('through headers exchange', $exchange);
            $numeric->expects($this->once())->method('execute')->with(
                $this->callback(static function ($msg) use ($number) {
                    return $msg->getBody() === 'through default exchange' && $msg->getRoutingKey() === $number;
                })
            );
            $anonymous->expects($this->once())->method('execute')->with(
                $this->callback(static function ($msg) { return $msg->getBody() === 'through headers exchange'; })
            );
            $this->assertSame(0, $config->getConsumer('worker')->consume(2));
            $routing = $config->getRouting($connection);
            $generated = $routing->getQueueName('');
            $this->assertStringStartsWith('amq.gen-', $generated);
            $this->assertTrue($routing->isQueueExists(''));
            $routing->deleteAll();
            $this->assertFalse($routing->isQueueExists($generated));
            $this->assertFalse($routing->isQueueExists($number));
            $routing->declareQueue('');
            $this->assertNotSame($generated, $routing->getQueueName(''));
            $connection->close();
            // Reopen the routing channel without declaring another anonymous queue.
            $this->assertTrue($routing->isExchangeExists('amq.topic'));
            $this->expectException(\mikemadisonweb\rabbitmq\exceptions\RuntimeException::class);
            $routing->getQueueName('');
        } finally {
            $connection->close();
            \Yii::$container->clear('anonymous-handler');
            \Yii::$container->clear('numeric-handler');
        }
    }

    public function testExternalResourcesAreBoundWithoutRedeclaration()
    {
        $external = $this->prefix . '-external';
        $manual = $this->prefix . '-manual';
        $config = $this->configure([
            'connections' => [['name' => 'default', 'url' => $this->url], ['name' => 'probe', 'url' => $this->url]],
            'exchanges' => [
                ['name' => $external, 'type' => 'fanout', 'declare' => false],
                ['name' => $manual, 'type' => 'direct', 'declare' => false],
            ],
            'queues' => [['name' => $external, 'declare' => false], ['name' => $manual, 'declare' => false]],
            'bindings' => [['exchange' => $external, 'queue' => $external, 'routing_keys' => ['job']]],
            'producers' => [['name' => 'sender']],
        ]);
        $connection = $config->getConnection();
        $routing = $config->getRouting($connection);
        $other = $config->getConnection('probe');
        try {
            $probe = $connection->channel();
            // Existing attributes deliberately differ from configuration; redeclaration would fail with 406.
            $probe->exchange_declare($external, 'direct', false, false, true);
            $probe->queue_declare($external, false, false, true, false);
            $producer = $config->getProducer('sender');
            $channel = $producer->getChannel();
            $channel->confirm_select();
            $producer->publish('external', $external, 'job');
            $channel->wait_for_pending_acks(3);
            $this->assertSame('external', $probe->basic_get($external, true)->getBody());
            $this->assertFalse($routing->isQueueExists($manual));
            $this->assertFalse($routing->isExchangeExists($manual));
            $routing->declareQueue($manual);
            $routing->declareExchange($manual);
            $this->assertTrue($routing->isQueueExists($manual));
            $this->assertTrue($routing->isExchangeExists($manual));
            try {
                $config->getRouting($other)->isQueueExists($external);
                $this->fail('An exclusive queue locked by another connection must report RESOURCE_LOCKED.');
            } catch (AMQPProtocolChannelException $e) {
                $this->assertSame(405, $e->getCode());
            }
        } finally {
            if ($routing->isQueueExists($manual)) {
                $routing->deleteQueue($manual);
            }
            if ($routing->isExchangeExists($manual)) {
                $routing->deleteExchange($manual);
            }
            $other->close();
            $connection->close();
        }
    }

    public function testCallbackReplacingTheSharedConnectionOnlyAcknowledgesTheRedelivery()
    {
        $queue = $this->prefix . '-redelivery';
        $handler = $this->createMock(ConsumerInterface::class);
        \Yii::$container->setSingleton('renew-handler', $handler);
        $config = $this->configure([
            'queues' => [['name' => $queue, 'durable' => true]],
            'consumers' => [['name' => 'worker', 'callbacks' => [$queue => 'renew-handler'], 'idle_timeout' => 2]],
            'producers' => [['name' => 'sender']],
        ]);
        $connection = $config->getConnection();
        $producer = $config->getProducer('sender');
        $consumer = $config->getConsumer('worker');
        $redeliveries = [];
        try {
            $producer->publish('replace the connection', '', $queue);
            $handler->expects($this->exactly(2))->method('execute')->willReturnCallback(
                static function (AMQPMessage $msg) use ($producer, &$redeliveries) {
                    $redeliveries[] = $msg->isRedelivered();
                    if (!$msg->isRedelivered()) {
                        $producer->renew();
                    }
                    return ConsumerInterface::MSG_ACK;
                }
            );
            $config->on('after_consume', static function () use ($connection) {
                $connection->close();
            });
            $this->assertSame(0, $consumer->consume(1));
            $this->assertSame([false, true], $redeliveries);
            $this->assertSame(1, $consumer->getConsumed());
            $this->assertNull($connection->channel()->basic_get($queue, true));
        } finally {
            $routing = $config->getRouting($connection);
            if ($routing->isQueueExists($queue)) {
                $routing->deleteQueue($queue);
            }
            $connection->close();
            \Yii::$container->clear('renew-handler');
        }
    }

    public function testConsecutiveLimitedConsumesReleasePrefetchedDeliveriesOnTheirOwnChannel()
    {
        $queues = [$this->prefix . '-first', $this->prefix . '-second'];
        $handler = $this->createMock(ConsumerInterface::class);
        \Yii::$container->setSingleton('limited-handler', $handler);
        $config = $this->configure([
            'queues' => [
                ['name' => $queues[0], 'exclusive' => true, 'durable' => false],
                ['name' => $queues[1], 'exclusive' => true, 'durable' => false],
            ],
            'producers' => [['name' => 'sender']],
            'consumers' => [[
                'name' => 'worker', 'callbacks' => array_fill_keys($queues, 'limited-handler'),
                'qos' => ['prefetch_count' => 10], 'idle_timeout' => 2, 'idle_timeout_exit_code' => 75,
            ]],
        ]);
        $received = [];
        $handler->method('execute')->willReturnCallback(static function (AMQPMessage $message) use (&$received) {
            $received[] = $message->getBody();
            return ConsumerInterface::MSG_ACK;
        });
        $connection = $config->getConnection();
        try {
            $producer = $config->getProducer('sender');
            $publisherChannel = $producer->getChannel();
            $publisherChannel->confirm_select();
            $producer->publish('a', '', $queues[0]);
            $producer->publish('b', '', $queues[0]);
            $producer->publish('c', '', $queues[1]);
            $publisherChannel->wait_for_pending_acks(3);
            $consumer = $config->getConsumer('worker');
            for ($i = 0; $i < 3; $i++) {
                $this->assertSame(0, $consumer->consume(1));
                $this->assertSame(1, $consumer->getConsumed());
            }
            sort($received);
            $this->assertSame(['a', 'b', 'c'], $received);
            $this->assertSame($publisherChannel, $producer->getChannel());
            foreach ($queues as $queue) {
                $this->assertNull($publisherChannel->basic_get($queue, true));
            }
        } finally {
            $connection->close();
        }
    }

    public function testIdleAndMemoryLimitLeaveNoActiveSubscriptions()
    {
        $queue = $this->prefix . '-idle';
        $handler = $this->createMock(ConsumerInterface::class);
        $handler->expects($this->never())->method('execute');
        \Yii::$container->setSingleton('idle-handler', $handler);
        $config = $this->configure([
            'queues' => [['name' => $queue, 'durable' => false, 'exclusive' => true]],
            'consumers' => [['name' => 'idle', 'callbacks' => [$queue => 'idle-handler'], 'idle_timeout' => 1]],
        ]);
        $consumer = $config->getConsumer('idle');
        try {
            $consumer->setMemoryLimit(1);
            $this->assertSame(0, $consumer->consume());
            $this->assertSame([], $consumer->getChannel()->callbacks);
            $consumer->setMemoryLimit(0);
            $consumer->setIdleTimeoutExitCode(22);
            $this->assertSame(22, $consumer->consume());
            $this->assertSame([], $consumer->getChannel()->callbacks);
            $consumer->setIdleTimeoutExitCode(null);
            try {
                $consumer->consume();
                $this->fail('Expected the configured idle timeout.');
            } catch (AMQPTimeoutException $expected) {
                $this->assertSame([], $consumer->getChannel()->callbacks);
            }
            $this->assertSame(0, $consumer->getConsumed());
        } finally {
            $consumer->close();
            \Yii::$container->clear('idle-handler');
        }
    }
}
