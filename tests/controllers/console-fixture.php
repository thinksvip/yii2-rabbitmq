<?php declare(strict_types=1);

use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\components\Consumer;
use mikemadisonweb\rabbitmq\DependencyInjection;
use yii\console\Application;

require dirname(__DIR__) . '/bootstrap.php';

$app = new Application([
    'id' => 'rabbitmq-console-fixture',
    'basePath' => __DIR__,
    'bootstrap' => [DependencyInjection::class],
    'components' => ['rabbitmq' => ['class' => Configuration::class, 'connections' => [['host' => 'unused']]]],
]);
Yii::$container->setSingleton(sprintf(Configuration::PRODUCER_SERVICE_NAME, 'local'), new class {
    public function publish($body, $exchange, $routingKey)
    {
        echo hash('sha256', $body), ' ', $exchange, ' ', $routingKey, "\n";
    }
});
Yii::$container->setSingleton(sprintf(Configuration::CONSUMER_SERVICE_NAME, 'local'), new class extends Consumer {
    public function __construct() {}
    public function consume($msgAmount = 0): int
    {
        return 0;
    }
});
exit($app->run());
