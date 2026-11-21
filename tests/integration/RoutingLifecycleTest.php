<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\integration;

use mikemadisonweb\rabbitmq\components\AbstractConnectionFactory;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\tests\TestCase;

class RoutingLifecycleTest extends TestCase
{
    public function testTemporaryRoutersReleaseTheirChannelsAndPreserveOtherUsers()
    {
        $url = getenv('RABBITMQ_TEST_URL_A');
        if (!$url) {
            $this->markTestSkipped('Set RABBITMQ_TEST_URL_A to a disposable test vhost.');
        }
        $this->loadExtension(['components' => ['rabbitmq' => [
            'class' => Configuration::class,
            'connections' => [['url' => $url]],
        ]]]);
        // An external connection deliberately bypasses the DI router cache.
        $options = array_replace(Configuration::DEFAULTS['connections'][0], [
            'url' => $url, 'channel_rpc_timeout' => 3.0,
        ]);
        $connection = (new AbstractConnectionFactory($options['type'], $options))->createConnection();
        try {
            $otherChannel = $connection->channel();
            [$queue] = $otherChannel->queue_declare('', false, false, true, false);
            $baseline = count($connection->channels);
            for ($i = 0; $i < 10; $i++) {
                $routing = \Yii::$app->rabbitmq->getRouting($connection);
                $this->assertTrue($routing->isQueueExists($queue));
                $this->assertCount($baseline + 1, $connection->channels);
                if ($i % 2 === 0) {
                    $routing->close();
                    $routing->close();
                    $this->assertCount($baseline, $connection->channels);
                    // close() clears local state, allowing this router to be reused.
                    $this->assertTrue($routing->isQueueExists($queue));
                }
                unset($routing);
                $this->assertCount($baseline, $connection->channels);
                $this->assertTrue($connection->isConnected());
                $this->assertSame($otherChannel, $connection->channels[$otherChannel->getChannelId()]);
            }
            $this->assertSame($queue, $otherChannel->queue_declare($queue, true)[0]);
        } finally {
            $connection->close();
        }
    }
}
