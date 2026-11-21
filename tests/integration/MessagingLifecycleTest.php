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
}
