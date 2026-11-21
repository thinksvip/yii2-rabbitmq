<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\components;

use BadFunctionCallException;
use ErrorException;
use mikemadisonweb\rabbitmq\events\RabbitMQConsumerEvent;
use mikemadisonweb\rabbitmq\exceptions\RuntimeException;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Message\AMQPMessage;
use Throwable;
use yii\console\Controller;

/**
 * Service that receives AMQP Messages
 *
 * @package mikemadisonweb\rabbitmq\components
 */
class Consumer extends BaseRabbitMQ
{
    protected $deserializer;

    protected $qos;

    protected $idleTimeout;

    protected $idleTimeoutExitCode;

    protected $queues = [];

    protected $memoryLimit = 0;

    protected $proceedOnException;

    protected $name = 'unnamed';

    private $id;

    private $target;

    private $consumed = 0;

    private $forceStop = false;

    private $pendingSignal;

    // wait() invokes application code synchronously. Only broker I/O failures may restart the session.
    private const HANDLING = 'handling';
    private const SETTLING = 'settling';
    private $deliveryPhase;

    /**
     * Set the memory limit
     *
     * @param int $memoryLimit
     */
    public function setMemoryLimit($memoryLimit)
    {
        $this->memoryLimit = $memoryLimit;
    }

    /**
     * Get the memory limit
     *
     * @return int
     */
    public function getMemoryLimit(): int
    {
        return $this->memoryLimit;
    }

    /**
     * @param array $queues
     */
    public function setQueues(array $queues)
    {
        $this->queues = $queues;
    }

    /**
     * @return array
     */
    public function getQueues(): array
    {
        return $this->queues;
    }

    /**
     * @param $idleTimeout
     */
    public function setIdleTimeout($idleTimeout)
    {
        $this->idleTimeout = $idleTimeout;
    }

    public function getIdleTimeout()
    {
        return $this->idleTimeout;
    }

    /**
     * Set exit code to be returned when there is a timeout exception
     *
     * @param int|null $idleTimeoutExitCode
     */
    public function setIdleTimeoutExitCode($idleTimeoutExitCode)
    {
        $this->idleTimeoutExitCode = $idleTimeoutExitCode;
    }

    /**
     * Get exit code to be returned when there is a timeout exception
     *
     * @return int|null
     */
    public function getIdleTimeoutExitCode()
    {
        return $this->idleTimeoutExitCode;
    }

    /**
     * @return mixed
     */
    public function getDeserializer(): callable
    {
        return $this->deserializer;
    }

    /**
     * @param mixed $deserializer
     */
    public function setDeserializer(callable $deserializer)
    {
        $this->deserializer = $deserializer;
    }

    /**
     * @return mixed
     */
    public function getQos(): array
    {
        return $this->qos;
    }

    /**
     * @param mixed $qos
     */
    public function setQos(array $qos)
    {
        $this->qos = $qos;
    }

    /**
     * @param string $name
     */
    public function setName(string $name)
    {
        $this->name = $name;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Resets the consumed property.
     * Use when you want to call start() or consume() multiple times.
     */
    public function getConsumed(): int
    {
        return $this->consumed;
    }

    /**
     * Resets the consumed property.
     * Use when you want to call start() or consume() multiple times.
     */
    public function resetConsumed()
    {
        $this->consumed = 0;
    }

    /**
     * @return mixed
     */
    public function getProceedOnException(): bool
    {
        return $this->proceedOnException;
    }

    /**
     * @param mixed $proceedOnException
     */
    public function setProceedOnException(bool $proceedOnException)
    {
        $this->proceedOnException = $proceedOnException;
    }

    /**
     * Consume designated number of messages (0 means infinite)
     *
     * @param int $msgAmount
     *
     * @return int
     * @throws BadFunctionCallException
     * @throws RuntimeException
     * @throws AMQPTimeoutException
     * @throws ErrorException
     */
    public function consume($msgAmount = 0): int
    {
        $this->target = $msgAmount;
        $this->resetConsumed();
        $this->deliveryPhase = null;

        try {
            return $this->runConsumer();
        } finally {
            // Cancelling subscriptions leaves prefetched deliveries unacked until their channel closes.
            $this->closeChannel();
        }
    }

    private function runConsumer(): int
    {
        $recovering = false;
        while (true) {
            $waiting = false;
            try {
                $this->setup();
                $this->getChannel();
                while ($this->ch->callbacks) {
                    if ($this->maybeStopConsumer()) {
                        return Controller::EXIT_CODE_NORMAL;
                    }
                    $this->applyPendingSignal();
                    if ($this->forceStop) {
                        return Controller::EXIT_CODE_NORMAL;
                    }

                    $waiting = true;
                    try {
                        $this->ch->wait(null, false, $this->getIdleTimeout());
                    } catch (AMQPTimeoutException $e) {
                        // A positive idle timeout can delay signal dispatch until wait() expires.
                        // Apply the requested lifecycle change only after that wait has unwound.
                        if ($this->deliveryPhase === null && $this->pendingSignal !== null && $this->hasCurrentChannel()) {
                            $waiting = false;
                            $this->applyPendingSignal();
                            if ($this->forceStop) {
                                return Controller::EXIT_CODE_NORMAL;
                            }
                            $recovering = false;
                            continue;
                        }
                        throw $e;
                    }
                    $waiting = false;
                    $recovering = false;
                    // A publisher called by a handler may have renewed the shared connection.
                    if (!$this->forceStop && !$this->hasCurrentChannel()) {
                        throw new AMQPChannelClosedException('Consumer channel closed during delivery.');
                    }
                    $this->applyPendingSignal();
                    if ($this->forceStop) {
                        return Controller::EXIT_CODE_NORMAL;
                    }
                }
                return Controller::EXIT_CODE_NORMAL;
            } catch (Throwable $e) {
                $phase = $this->deliveryPhase;
                $this->deliveryPhase = null;
                $restartRequested = $this->pendingSignal !== null && $this->pendingSignal === SIGHUP;
                $stopRequested = $this->pendingSignal !== null && $this->pendingSignal !== SIGHUP;
                $this->pendingSignal = null;
                if ($phase === self::HANDLING || !$this->isTransportFailure($e)) {
                    throw $e;
                }
                if ($stopRequested) {
                    $this->forceStop = true;
                }
                if ($this->forceStop) {
                    return Controller::EXIT_CODE_NORMAL;
                }
                if ($e instanceof AMQPTimeoutException && $waiting && $phase === null && !$restartRequested) {
                    if ($this->hasCurrentChannel()) {
                        $this->stopConsuming();
                    }
                    if ($this->getIdleTimeoutExitCode() !== null) {
                        return $this->getIdleTimeoutExitCode();
                    }
                    throw $e;
                }
                if ($recovering) {
                    throw $e;
                }

                $recovering = true;
                $this->renew();
            }
        }
    }

    /** Only record intent: both an active delivery and an active wait must finish first. */
    public function handleSignal(int $signal): void
    {
        if ($this->pendingSignal === null || $this->pendingSignal === SIGHUP) {
            $this->pendingSignal = $signal;
        }
    }

    private function applyPendingSignal(): void
    {
        $signal = $this->pendingSignal;
        $this->pendingSignal = null;
        if ($signal === null || $this->forceStop) {
            return;
        }
        if ($signal === SIGHUP) {
            $this->restartDaemon();
        } else {
            $this->stopDaemon();
        }
    }

    /**
     * Stop consuming messages
     */
    public function stopConsuming()
    {
        foreach ($this->queues as $name => $options)
        {
            $this->getChannel()->basic_cancel($this->getConsumerTag((string)$name), false, true);
        }
    }

    /**
     * Force stop the consumer
     */
    public function stopDaemon()
    {
        $this->forceStop = true;
        $this->stopConsuming();
        $this->logger->printInfo("\nConsumer stopped by user.\n");
    }

    /**
     * Force restart the consumer
     */
    public function restartDaemon()
    {
        $this->stopConsuming();
        $this->renew();
        $this->setup();
        $this->logger->printInfo("\nConsumer has been restarted.\n");
    }

    /**
     * Sets the qos settings for the current channel
     * This method needs a connection to broker
     */
    protected function setQosOptions()
    {
        if (empty($this->qos))
        {
            return;
        }
        $prefetchSize  = $this->qos['prefetch_size'] ?? null;
        $prefetchCount = $this->qos['prefetch_count'] ?? null;
        $global        = $this->qos['global'] ?? null;
        $this->getChannel()->basic_qos($prefetchSize, $prefetchCount, $global);
    }

    /**
     * Start consuming messages
     *
     * @throws RuntimeException
     */
    protected function startConsuming()
    {
        $this->id = $this->generateUniqueId();
        foreach ($this->queues as $queue => $callback)
        {
            $that = $this;
            $queue = (string)$queue;
            $this->getChannel()->basic_consume(
                $queue === '' ? $this->routing->getQueueName($queue) : $queue,
                $this->getConsumerTag($queue),
                null,
                null,
                null,
                null,
                function (AMQPMessage $msg) use ($that, $queue, $callback)
                {
                    // Execute user-defined callback
                    $that->onReceive($msg, $queue, $callback);
                }
            );
        }
    }

    /**
     * Decide whether it's time to stop consuming
     *
     * @throws BadFunctionCallException
     */
    protected function maybeStopConsumer(): bool
    {
        if (extension_loaded('pcntl') && (defined('AMQP_WITHOUT_SIGNALS') ? !AMQP_WITHOUT_SIGNALS : true))
        {
            if (!function_exists('pcntl_signal_dispatch'))
            {
                throw new BadFunctionCallException(
                    "Function 'pcntl_signal_dispatch' is referenced in the php.ini 'disable_functions' and can't be called."
                );
            }
            pcntl_signal_dispatch();
        }
        if ($this->forceStop || ($this->consumed === $this->target && $this->target > 0))
        {
            $this->stopConsuming();

            return true;
        }

        if (0 !== $this->getMemoryLimit() && $this->isRamAlmostOverloaded())
        {
            $this->stopConsuming();

            return true;
        }

        return false;
    }

    /**
     * Callback that will be fired upon receiving new message
     *
     * @param AMQPMessage $msg
     * @param             $queueName
     * @param             $callback
     *
     * @return bool
     * @throws Throwable
     */
    protected function onReceive(AMQPMessage $msg, string $queueName, callable $callback): bool
    {
        $this->deliveryPhase = self::HANDLING;
        $heartbeat = ConsumerHeartbeat::start($this->conn);
        try {
            $this->processDelivery($msg, $queueName, $callback, $heartbeat);
        } finally {
            if ($heartbeat !== null) {
                $heartbeat->stop();
            }
        }
        $this->consumed++;
        if ($heartbeat !== null) {
            // A long after_consume listener may have lost the connection after the ACK.
            $this->deliveryPhase = self::SETTLING;
            $heartbeat->throwIfFailed();
        }
        $this->deliveryPhase = null;
        return true;
    }

    private function processDelivery(AMQPMessage $msg, string $queueName, callable $callback, ?ConsumerHeartbeat $heartbeat): void
    {
        $timeStart = microtime(true);
        \Yii::$app->rabbitmq->trigger(
            RabbitMQConsumerEvent::BEFORE_CONSUME,
            new RabbitMQConsumerEvent(
                [
                    'message'  => $msg,
                    'consumer' => $this,
                ]
            )
        );

        $processingFailed = false;
        try
        {
            // deserialize message back to initial data type
            if ($msg->has('application_headers') &&
                isset($msg->get('application_headers')->getNativeData()['rabbitmq.serialized']))
            {
                $msg->setBody(call_user_func($this->deserializer, $msg->getBody()));
            }
            // process message and return the result code back to broker
            $processFlag = $callback($msg);
        } catch (Throwable $e) {
            $this->logger->logError($e, $msg);
            if (!$this->proceedOnException) {
                throw $e;
            }
            // Release the prefetch slot while preserving the failed message for redelivery.
            $processFlag = ConsumerInterface::MSG_REQUEUE;
            $processingFailed = true;
        }

        try {
            $this->deliveryPhase = self::SETTLING;
            if ($heartbeat !== null) {
                $heartbeat->throwIfFailed();
            }
            $this->sendResult($msg, $processFlag);
            $this->deliveryPhase = self::HANDLING;
            if ($processingFailed) {
                return;
            }
            \Yii::$app->rabbitmq->trigger(
                RabbitMQConsumerEvent::AFTER_CONSUME,
                new RabbitMQConsumerEvent(
                    [
                        'message'  => $msg,
                        'consumer' => $this,
                    ]
                )
            );

            $this->logger->printResult($queueName, $processFlag, $timeStart);
            $this->logger->log(
                'Queue message processed.',
                $msg,
                [
                    'queue'       => $queueName,
                    'processFlag' => $processFlag,
                    'timeStart'   => $timeStart,
                    'memory'      => true,
                ]
            );
        }
        catch (Throwable $e)
        {
            $this->logger->logError($e, $msg);
            if (!$this->proceedOnException || ($this->deliveryPhase === self::SETTLING && $this->isTransportFailure($e)))
            {
                throw $e;
            }
        }
    }

    /**
     * Mark message status based on return code from callback
     *
     * @param AMQPMessage $msg
     * @param             $processFlag
     */
    protected function sendResult(AMQPMessage $msg, $processFlag)
    {
        // true in testing environment
        if ($msg->getChannel() === null)
        {
            return;
        }

        // A delivery tag is valid only on the channel of the original delivery.
        if ($msg->getChannel() !== $this->ch || !$this->hasCurrentChannel()) {
            throw new AMQPChannelClosedException('Delivery channel closed before acknowledgement.');
        }

        // respond to the broker with appropriate reply code
        if ($processFlag === ConsumerInterface::MSG_REQUEUE || false === $processFlag)
        {
            // Reject and requeue message to RabbitMQ
            $msg->reject(true);
        }
        elseif ($processFlag === ConsumerInterface::MSG_REJECT)
        {
            // Reject and drop
            $msg->reject(false);
        }
        else
        {
            // Remove message from queue only if callback return not false
            $msg->ack();
        }
    }

    /**
     * Checks if memory in use is greater or equal than memory allowed for this process
     *
     * @return boolean
     */
    protected function isRamAlmostOverloaded(): bool
    {
        return memory_get_usage(true) >= ($this->getMemoryLimit() * 1024 * 1024);
    }

    /**
     * @param string $queueName
     *
     * @return string
     */
    protected function getConsumerTag(string $queueName): string
    {
        return sprintf('%s-%s-%s', $queueName, $this->name, $this->id);
    }

    /**
     * @return string
     */
    protected function generateUniqueId(): string
    {
        return uniqid('rabbitmq_', true);
    }

    protected function setup()
    {
        if ($this->autoDeclare)
        {
            $this->routing->declareAll();
        }
        $this->setQosOptions();
        $this->startConsuming();
    }
}
