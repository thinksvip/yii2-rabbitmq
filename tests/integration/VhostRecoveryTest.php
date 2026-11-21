<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\integration;

use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;

class VhostRecoveryTest extends TestCase
{
    public function testAutoDeclareOnTwoVhostsAndRecreateExclusiveQueueAfterDisconnect()
    {
        $urls = ['a' => getenv('RABBITMQ_TEST_URL_A'), 'b' => getenv('RABBITMQ_TEST_URL_B')];
        if (!$urls['a'] || !$urls['b']) {
            $this->markTestSkipped('Set RABBITMQ_TEST_URL_A and RABBITMQ_TEST_URL_B to two test vhosts.');
        }

        $config = ['class' => Configuration::class];
        $prefix = 'yii-test-' . bin2hex(random_bytes(8));
        $exchange = $prefix . '-events';
        foreach ($urls as $name => $url) {
            $queue = "$prefix-$name";
            $config['connections'][] = ['name' => $name, 'url' => $url];
            $config['producers'][] = ['name' => $name, 'connection' => $name];
            $config['exchanges'][] = [
                'name' => $exchange, 'connection' => $name, 'type' => 'direct',
                'durable' => false, 'auto_delete' => true,
            ];
            $config['queues'][] = [
                'name' => $queue, 'connection' => $name, 'durable' => false, 'exclusive' => true,
            ];
            $config['bindings'][] = ['queue' => $queue, 'exchange' => $exchange, 'routing_keys' => ['event']];
        }

        $this->loadExtension(['components' => ['rabbitmq' => $config]]);
        $rabbitmq = \Yii::$app->rabbitmq;
        foreach (['b', 'a'] as $name) {
            $rabbitmq->getProducer($name)->publish($name, $exchange, 'event');
            $channel = $rabbitmq->getConnection($name)->channel();
            $this->assertSame($name, $channel->basic_get("$prefix-$name", true)->getBody());
            $channel->close();
        }
        foreach (['a' => 'b', 'b' => 'a'] as $name => $other) {
            $channel = $rabbitmq->getConnection($name)->channel();
            try {
                $channel->queue_declare("$prefix-$other", true);
                $this->fail("Found $other's queue on vhost $name");
            } catch (AMQPProtocolChannelException $e) {
                $this->assertSame(404, $e->getCode());
            }
        }

        // Closing the connection removes its exclusive queue and the binding.
        $rabbitmq->getConnection('a')->close();
        $rabbitmq->getProducer('a')->publish('after reconnect', $exchange, 'event');
        $channel = $rabbitmq->getConnection('a')->channel();
        $this->assertSame('after reconnect', $channel->basic_get("$prefix-a", true)->getBody());
        $this->assertNull($channel->basic_get("$prefix-a", true));

        $rabbitmq->on('before_publish', function ($event) {
            $event->producer->renew();
        });
        $rabbitmq->getProducer('a')->publish('after handler renew', $exchange, 'event');
        $channel = $rabbitmq->getConnection('a')->channel();
        $this->assertSame('after handler renew', $channel->basic_get("$prefix-a", true)->getBody());
    }
}
