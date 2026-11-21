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
}
