<?php declare(strict_types=1);

use mikemadisonweb\rabbitmq\components\ConsumerInterface;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\DependencyInjection;
use PhpAmqpLib\Message\AMQPMessage;

define('YII_ENABLE_ERROR_HANDLER', false);
require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/vendor/yiisoft/yii2/Yii.php';

class InstalledConsumer implements ConsumerInterface
{
    public static $received = [];

    public function execute(AMQPMessage $message)
    {
        self::$received[] = $message->getBody();
        return self::MSG_ACK;
    }
}

$extensions = require __DIR__ . '/vendor/yiisoft/extensions.php';
if (realpath(__DIR__ . '/extension') !== realpath(__DIR__ . '/vendor/xincheng/yii2-rabbitmq')) {
    throw new RuntimeException('Composer did not install the package from the local checkout.');
}
if (($extensions['xincheng/yii2-rabbitmq']['bootstrap'] ?? null) !== DependencyInjection::class) {
    throw new RuntimeException('Composer did not register the extension bootstrap.');
}

$mode = $argv[1];
$queue = 'yii-install-' . bin2hex(random_bytes(8));
$config = [
    'id' => 'installed-' . $mode,
    'basePath' => __DIR__,
    'components' => ['rabbitmq' => [
        'class' => Configuration::class,
        'connections' => [['url' => getenv('RABBITMQ_TEST_URL_A')]],
        'queues' => [['name' => $queue, 'exclusive' => true, 'durable' => false]],
        'producers' => [['name' => 'sender', 'safe' => false]],
        'consumers' => [['name' => 'worker', 'callbacks' => [$queue => InstalledConsumer::class], 'idle_timeout' => 5]],
        'logger' => ['print_console' => false],
    ]],
];
if ($mode === 'web') {
    $config['components']['request'] = ['scriptFile' => __FILE__, 'scriptUrl' => '/index.php'];
    $app = new yii\web\Application($config);
} else {
    $app = new yii\console\Application($config);
}

$rabbitmq = $app->rabbitmq;
$worker = $rabbitmq->getConsumer('worker');
try {
    // Auto-declare the queue, then publish through RabbitMQ's default exchange.
    $rabbitmq->getProducer('sender')->publish($mode, '', $queue);
    $worker->consume(1);
    $worker->getChannel()->close();
    $probe = $rabbitmq->getConnection()->channel();
    if (InstalledConsumer::$received !== [$mode] || $probe->basic_get($queue, true) !== null) {
        throw new RuntimeException("The installed $mode application did not acknowledge its delivery.");
    }
    echo "Installed Yii $mode application delivered and acknowledged the message.\n";
} finally {
    $worker->close();
}
