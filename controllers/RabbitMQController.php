<?php declare(strict_types=1);
declare(ticks=1);

namespace mikemadisonweb\rabbitmq\controllers;

use BadFunctionCallException;
use InvalidArgumentException;
use mikemadisonweb\rabbitmq\components\Consumer;
use mikemadisonweb\rabbitmq\components\Routing;
use mikemadisonweb\rabbitmq\Configuration;
use Yii;
use yii\base\Action;
use yii\console\Controller;
use yii\console\ExitCode;
use yii\helpers\Console;

/**
 * RabbitMQ extension functionality
 *
 * @package mikemadisonweb\rabbitmq\controllers
 */
class RabbitMQController extends Controller
{
    public $memoryLimit;

    public $messagesLimit;

    public $debug;

    public $withoutSignals;

    /**
     * @var Configuration
     */
    private $rabbitmq;

    public function init()
    {
        $this->rabbitmq = Yii::$app->rabbitmq;
    }

    protected $options = [
        'm' => 'messagesLimit',
        'l' => 'memoryLimit',
        'd' => 'debug',
        'w' => 'withoutSignals',
    ];

    /**
     * @param string $actionID
     *
     * @return array
     */
    public function options($actionID): array
    {
        return array_merge(parent::options($actionID), array_values($this->options));
    }

    /**
     * @return array
     */
    public function optionAliases(): array
    {
        return array_merge(parent::optionAliases(), $this->options);
    }

    /**
     * @param Action $event
     *
     * @return bool
     */
    public function beforeAction($event): bool
    {
        if (defined('AMQP_WITHOUT_SIGNALS') === false)
        {
            define('AMQP_WITHOUT_SIGNALS', filter_var($this->withoutSignals, FILTER_VALIDATE_BOOLEAN));
        }
        if (defined('AMQP_DEBUG') === false)
        {
            define('AMQP_DEBUG', filter_var($this->debug, FILTER_VALIDATE_BOOLEAN));
        }

        return parent::beforeAction($event);
    }

    /**
     * Run a consumer
     *
     * @param    string $name Consumer name
     *
     * @return   int
     * @throws \Throwable
     */
    public function actionConsume(string $name): int
    {
        return $this->runConsumer($name, false);
    }

    /**
     * Restart consumer by name
     *
     * @param string $name
     * @return int
     * @throws \Throwable
     * @throws \yii\base\InvalidConfigException
     * @throws \yii\di\NotInstantiableException
     */
    public function actionRestartConsume(string $name): int
    {
        return $this->runConsumer($name, true);
    }

    private function runConsumer(string $name, bool $restart): int
    {
        $memoryLimit = $this->getLimit($this->memoryLimit, '-l');
        $messagesLimit = $this->getLimit($this->messagesLimit, '-m');
        $consumer = $this->rabbitmq->getConsumer($name);
        if ($restart) {
            $consumer->renew();
        }
        $this->registerConsumerSignals($consumer);
        if ($memoryLimit > 0) {
            $consumer->setMemoryLimit($memoryLimit);
        }
        return $consumer->consume($messagesLimit);
    }

    /**
     * Publish a message from STDIN to the queue
     *
     * @param string $producerName
     * @param string $exchangeName
     * @param string $routingKey
     *
     * @return int
     * @throws \yii\base\InvalidConfigException
     * @throws \yii\di\NotInstantiableException
     */
    public function actionPublish(string $producerName, string $exchangeName, string $routingKey = ''): int
    {
        $producer = $this->rabbitmq->getProducer($producerName);
        $data = '';
        if (posix_isatty(STDIN))
        {
            $this->stderr(Console::ansiFormat("Please pipe in some data in order to send it.\n", [Console::FG_RED]));

            return ExitCode::UNSPECIFIED_ERROR;
        }
        while (!feof(STDIN))
        {
            $data .= fread(STDIN, 8192);
        }
        $producer->publish($data, $exchangeName, $routingKey);
        $this->stdout("Message was successfully published.\n", Console::FG_GREEN);

        return ExitCode::OK;
    }

    /**
     * Create RabbitMQ exchanges, queues and bindings based on configuration
     *
     * @param string $connectionName
     *
     * @return int
     * @throws \RuntimeException
     */
    public function actionDeclareAll(string $connectionName = Configuration::DEFAULT_CONNECTION_NAME): int
    {
        $routing = $this->getRouting($connectionName);
        $result  = $routing->declareAll();
        if ($result)
        {
            $this->stdout(
                Console::ansiFormat("All configured entries was successfully declared.\n", [Console::FG_GREEN])
            );

            return ExitCode::OK;
        }
        $this->stderr(Console::ansiFormat("No queues, exchanges or bindings configured.\n", [Console::FG_RED]));

        return ExitCode::UNSPECIFIED_ERROR;
    }

    /**
     * Create the exchange listed in configuration
     *
     * @param        $exchangeName
     * @param string $connectionName
     *
     * @return int
     * @throws \RuntimeException
     */
    public function actionDeclareExchange(
        string $exchangeName,
        string $connectionName = Configuration::DEFAULT_CONNECTION_NAME
    ): int {
        $routing = $this->getRouting($connectionName);
        if ($routing->isExchangeExists($exchangeName))
        {
            $this->stderr(Console::ansiFormat("Exchange `{$exchangeName}` is already exists.\n", [Console::FG_RED]));

            return ExitCode::UNSPECIFIED_ERROR;
        }
        $routing->declareExchange($exchangeName);
        $this->stdout(Console::ansiFormat("Exchange `{$exchangeName}` was declared.\n", [Console::FG_GREEN]));

        return ExitCode::OK;
    }

    /**
     * Create the queue listed in configuration
     *
     * @param        $queueName
     * @param string $connectionName
     *
     * @return int
     * @throws \RuntimeException
     */
    public function actionDeclareQueue(
        string $queueName,
        string $connectionName = Configuration::DEFAULT_CONNECTION_NAME
    ): int {
        $routing = $this->getRouting($connectionName);
        if ($queueName !== '' && $routing->isQueueExists($queueName))
        {
            $this->stderr(Console::ansiFormat("Queue `{$queueName}` is already exists.\n", [Console::FG_RED]));

            return ExitCode::UNSPECIFIED_ERROR;
        }
        $routing->declareQueue($queueName);
        $message = $queueName === '' ? "Server-named queue was declared.\n" : "Queue `{$queueName}` was declared.\n";
        $this->stdout(Console::ansiFormat($message, [Console::FG_GREEN]));

        return ExitCode::OK;
    }

    /**
     * Delete all RabbitMQ exchanges and queues that is defined in configuration
     *
     * @param string $connection
     *
     * @return int
     * @throws \RuntimeException
     */
    public function actionDeleteAll(string $connection = Configuration::DEFAULT_CONNECTION_NAME): int
    {
        if ($this->interactive)
        {
            $input = Console::prompt('Are you sure you want to delete all queues and exchanges?', ['default' => 'yes']);
            if ($input !== 'yes' && $input !== 'y')
            {
                $this->stderr(Console::ansiFormat("Aborted.\n", [Console::FG_RED]));

                return ExitCode::UNSPECIFIED_ERROR;
            }
        }
        $routing = $this->getRouting($connection);
        $routing->deleteAll();
        $this->stdout(Console::ansiFormat("All configured entries was deleted.\n", [Console::FG_GREEN]));

        return ExitCode::OK;
    }

    /**
     * Delete an exchange
     *
     * @param        $exchangeName
     * @param string $connectionName
     *
     * @return int
     * @throws \RuntimeException
     */
    public function actionDeleteExchange(
        string $exchangeName,
        string $connectionName = Configuration::DEFAULT_CONNECTION_NAME
    ): int {
        if ($this->interactive)
        {
            $input = Console::prompt('Are you sure you want to delete that exchange?', ['default' => 'yes']);
            if ($input !== 'yes')
            {
                $this->stderr(Console::ansiFormat("Aborted.\n", [Console::FG_RED]));

                return ExitCode::UNSPECIFIED_ERROR;
            }
        }
        $routing = $this->getRouting($connectionName);
        $routing->deleteExchange($exchangeName);
        $this->stdout(Console::ansiFormat("Exchange `{$exchangeName}` was deleted.\n", [Console::FG_GREEN]));

        return ExitCode::OK;
    }

    /**
     * Delete a queue
     *
     * @param        $queueName
     * @param string $connectionName
     *
     * @return int
     * @throws \RuntimeException
     */
    public function actionDeleteQueue(
        string $queueName,
        string $connectionName = Configuration::DEFAULT_CONNECTION_NAME
    ): int {
        if ($this->interactive)
        {
            $input = Console::prompt('Are you sure you want to delete that queue?', ['default' => 'yes']);
            if ($input !== 'yes')
            {
                $this->stderr(Console::ansiFormat("Aborted.\n", [Console::FG_RED]));

                return ExitCode::UNSPECIFIED_ERROR;
            }
        }

        $routing = $this->getRouting($connectionName);
        $routing->deleteQueue($queueName);
        $this->stdout(Console::ansiFormat("Queue `{$queueName}` was deleted.\n", [Console::FG_GREEN]));

        return ExitCode::OK;
    }

    /**
     * Delete all messages from the queue
     *
     * @param        $queueName
     * @param string $connectionName
     *
     * @return int
     * @throws \RuntimeException
     */
    public function actionPurgeQueue(
        string $queueName,
        string $connectionName = Configuration::DEFAULT_CONNECTION_NAME
    ): int {
        if ($this->interactive)
        {
            $input = Console::prompt(
                'Are you sure you want to delete all messages inside that queue?',
                ['default' => 'yes']
            );
            if ($input !== 'yes')
            {
                $this->stderr(Console::ansiFormat("Aborted.\n", [Console::FG_RED]));

                return ExitCode::UNSPECIFIED_ERROR;
            }
        }

        $routing = $this->getRouting($connectionName);
        $routing->purgeQueue($queueName);
        $this->stdout(Console::ansiFormat("Queue `{$queueName}` was purged.\n", [Console::FG_GREEN]));

        return ExitCode::OK;
    }


    /**
     * @param string $connectionName
     * @return Routing|object|string
     * @throws \yii\base\InvalidConfigException
     * @throws \yii\di\NotInstantiableException
     */
    private function getRouting(string $connectionName)
    {
        $conn = $this->rabbitmq->getConnection($connectionName);
        return $this->rabbitmq->getRouting($conn);
    }

    /**
     * Register signal handlers for this worker
     *
     * @param Consumer $consumer
     */
    private function registerConsumerSignals(Consumer $consumer)
    {
        if (!AMQP_WITHOUT_SIGNALS && extension_loaded('pcntl'))
        {
            if (!function_exists('pcntl_signal'))
            {
                throw new BadFunctionCallException(
                    "Function 'pcntl_signal' is referenced in the php.ini 'disable_functions' and can't be called."
                );
            }

            pcntl_signal(SIGTERM, [$consumer, 'handleSignal']);
            pcntl_signal(SIGINT, [$consumer, 'handleSignal']);
            pcntl_signal(SIGHUP, [$consumer, 'handleSignal']);
        }
    }

    private function getLimit($value, string $option): int
    {
        $value = $value ?? 0;
        if ((!is_int($value) && !is_string($value)) || !ctype_digit((string)$value)) {
            throw new InvalidArgumentException("The {$option} option must be a non-negative integer.");
        }
        return (int)$value;
    }
}
