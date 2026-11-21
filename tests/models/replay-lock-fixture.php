<?php declare(strict_types=1);

require dirname(__DIR__) . '/bootstrap.php';

use mikemadisonweb\rabbitmq\components\Producer;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\migrations\M201026132305RabbitPublishError;
use mikemadisonweb\rabbitmq\models\RabbitPublishError;

class BlockingReplayProducer extends Producer
{
    public function __construct()
    {
    }

    public function publish($msgBody, string $exchangeName, string $routingKey = '', array $additionalProperties = [], ?array $headers = null)
    {
        fwrite(STDOUT, "locked\n");
        fflush(STDOUT);
        while (true) {
            usleep(100000);
        }
    }
}

new yii\console\Application([
    'id' => 'replay-lock-fixture', 'basePath' => __DIR__, 'runtimePath' => $argv[1] . '/runtime',
    'components' => [
        'db' => ['class' => yii\db\Connection::class, 'dsn' => 'sqlite::memory:'],
        'rabbitmq' => ['class' => Configuration::class],
        RabbitPublishError::MUTEX_COMPONENT => ['class' => yii\mutex\FileMutex::class, 'mutexPath' => $argv[1]],
    ],
]);
(new M201026132305RabbitPublishError(['compact' => true]))->up();
Yii::$container->setSingleton(sprintf(Configuration::PRODUCER_SERVICE_NAME, 'worker'), new BlockingReplayProducer());
$item = new RabbitPublishError([
    'msgBody' => 'locked', 'exchangeName' => '', 'producerName' => 'worker', 'errorMsg' => 'initial failure',
]);
$item->saveItem();
$item->rePublish();
