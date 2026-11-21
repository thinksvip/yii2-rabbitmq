<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\components;

use mikemadisonweb\rabbitmq\exceptions\RuntimeException;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;

class Routing
{
    use ChannelCache;

    protected $queues = [];
    protected $exchanges = [];
    protected $bindings = [];

    private $exchangesDeclared = [];
    private $queuesDeclared = [];
    private $generatedQueueName;
    private $ownedExclusiveQueues = [];

    // RabbitMQ may finish deleting a named exclusive queue just after its owning connection closes.
    private const EXCLUSIVE_RELEASE_DELAYS_US = [20000, 50000, 100000, 200000];
    private $isDeclared = false;

    /**
     * @param AbstractConnection $conn
     */
    public function __construct(AbstractConnection $conn)
    {
        $this->conn = $conn;
    }

    /**
     * @param array $queues
     */
    public function setQueues(array $queues)
    {
        $this->queues = $this->arrangeByName($queues);
    }

    /**
     * @param array $exchanges
     */
    public function setExchanges(array $exchanges)
    {
        $this->exchanges = array_column($exchanges, null, 'name');
    }

    /**
     * @param array $bindings
     */
    public function setBindings(array $bindings)
    {
        $this->bindings = $bindings;
    }

    /**
     * Declare all routing entries defined by configuration
     * @return bool
     * @throws RuntimeException
     */
    public function declareAll() : bool
    {
        if ($this->ch !== null && !$this->hasCurrentChannel()) {
            $this->reset();
        }
        if (!$this->isDeclared) {
            // Deleting an old exclusive queue can remove its auto-delete exchange. Reacquire queues first.
            foreach ($this->queues as $name => $queue) {
                if ($name === '') {
                    $this->declareQueueEntries('', array_filter($queue, static function ($entry) {
                        return $entry['declare'] ?? true;
                    }));
                } elseif ($queue['declare'] ?? true) {
                    $this->declareQueue((string)$name);
                }
            }
            foreach ($this->exchanges as $name => $exchange) {
                if ($name !== '' && ($exchange['declare'] ?? true)) {
                    $this->declareExchange((string)$name);
                }
            }
            $this->declareBindings();
            $this->isDeclared = true;

            return true;
        }

        return false;
    }

    /**
     * @param $queueName
     * @throws RuntimeException
     */
    public function declareQueue(string $queueName)
    {
        if(!isset($this->queues[$queueName])) {
            throw new RuntimeException("Queue `{$queueName}` is not configured.");
        }

        $queue = $this->queues[$queueName];
        $this->declareQueueEntries($queueName, $queueName === '' ? $queue : [$queue]);
    }

    private function declareQueueEntries(string $queueName, array $entries): void
    {
        foreach ($entries as $index => $queue) {
            $channel = $this->getChannel();
            if (isset($this->queuesDeclared[$queueName][$index])) {
                continue;
            }
            $result = $this->declareQueueOnBroker($channel, $queue);
            if ($queueName === '' && $result !== null) {
                $this->generatedQueueName = $result[0];
            }
            $this->queuesDeclared[$queueName][$index] = true;
        }
    }

    private function declareQueueOnBroker(AMQPChannel $channel, array $queue)
    {
        $name = $queue['name'];
        $wasOwner = isset($this->ownedExclusiveQueues[$name]);
        unset($this->ownedExclusiveQueues[$name]);
        for ($attempt = 0; ; $attempt++) {
            try {
                $result = $channel->queue_declare(
                    $name, $queue['passive'], $queue['durable'], $queue['exclusive'],
                    $queue['auto_delete'], $queue['nowait'], $queue['arguments'], $queue['ticket']
                );
                if ($name !== '' && $queue['exclusive'] && !$queue['passive'] && !$queue['nowait']) {
                    $this->ownedExclusiveQueues[$name] = true;
                }
                return $result;
            } catch (AMQPProtocolChannelException $e) {
                $this->replaceClosedChannel();
                if (!$wasOwner || !$queue['exclusive'] || $queue['passive'] || $e->getCode() !== 405
                    || !isset(self::EXCLUSIVE_RELEASE_DELAYS_US[$attempt])) {
                    throw $e;
                }
                usleep(self::EXCLUSIVE_RELEASE_DELAYS_US[$attempt]);
                $channel = $this->getChannel();
            }
        }
    }

    /** The broker's empty-name shorthand only works on the channel which declared the queue. */
    public function getQueueName(string $name): string
    {
        if ($name !== '') {
            return $name;
        }
        if ($this->generatedQueueName === null || !$this->hasCurrentChannel()) {
            throw new RuntimeException('Declare the server-named queue with nowait=false on this connection first.');
        }
        return $this->generatedQueueName;
    }

    /**
     * Create bindings
     */
    public function declareBindings()
    {
        foreach ($this->bindings as $binding) {
            if (isset($binding['queue'])) {
                $this->bindExchangeToQueue($binding);
            } else {
                $this->bindExchangeToExchange($binding);
            }
        }
    }

    /**
     * Create exchange-to-queue binding
     * @param array $binding
     */
    public function bindExchangeToQueue(array $binding)
    {
        if (isset($binding['routing_keys']) && count($binding['routing_keys']) > 0) {
            foreach ($binding['routing_keys'] as $routingKey) {
                // queue binding is not permitted on the default exchange
                if ('' !== $binding['exchange']) {
                    $this->getChannel()->queue_bind($this->getQueueName($binding['queue']), $binding['exchange'], $routingKey);
                }
            }
        } else {
            // queue binding is not permitted on the default exchange
            if ('' !== $binding['exchange']) {
                $this->getChannel()->queue_bind($this->getQueueName($binding['queue']), $binding['exchange']);
            }
        }
    }

    /**
     * Create exchange-to-exchange binding
     * @param array $binding
     */
    public function bindExchangeToExchange(array $binding)
    {
        if (isset($binding['routing_keys']) && count($binding['routing_keys']) > 0) {
            foreach ($binding['routing_keys'] as $routingKey) {
                // queue binding is not permitted on the default exchange
                if ('' !== $binding['exchange']) {
                    $this->getChannel()->exchange_bind($binding['to_exchange'], $binding['exchange'], $routingKey);
                }
            }
        } else {
            // queue binding is not permitted on the default exchange
            if ('' !== $binding['exchange']) {
                $this->getChannel()->exchange_bind($binding['to_exchange'], $binding['exchange']);
            }
        }
    }

    /**
     * @param $exchangeName
     * @throws RuntimeException
     */
    public function declareExchange(string $exchangeName)
    {
        if(!isset($this->exchanges[$exchangeName])) {
            throw new RuntimeException("Exchange `{$exchangeName}` is not configured.");
        }
        $channel = $this->getChannel();
        $exchange = $this->exchanges[$exchangeName];
        if (!isset($this->exchangesDeclared[$exchangeName])) {
            $channel->exchange_declare(
                $exchange['name'],
                $exchange['type'],
                $exchange['passive'],
                $exchange['durable'],
                $exchange['auto_delete'],
                $exchange['internal'],
                $exchange['nowait'],
                $exchange['arguments'],
                $exchange['ticket']
            );
            $this->exchangesDeclared[$exchangeName] = true;
        }
    }

    /**
     * Purge the queue
     * @param string $queueName
     * @throws RuntimeException
     */
    public function purgeQueue(string $queueName)
    {
        if (!isset($this->queues[$queueName])) {
            throw new RuntimeException("Queue {$queueName} is not configured. Purge is aborted.");
        }
        $this->getChannel()->queue_purge($this->getQueueName($queueName), true);
    }

    /**
     * Delete all configured queues and exchanges
     * @throws RuntimeException
     */
    public function deleteAll()
    {
        foreach (array_keys($this->queues) as $name) {
            // A different session cannot identify an anonymous queue from configuration alone.
            if ($name !== '' || ($this->generatedQueueName !== null && $this->hasCurrentChannel())) {
                $this->deleteQueue((string)$name);
            }
        }
        foreach (array_keys($this->exchanges) as $name) {
            if ($name !== '') {
                $this->deleteExchange((string)$name);
            }
        }
    }

    /**
     * Delete the queue
     * @param string $queueName
     * @throws RuntimeException
     */
    public function deleteQueue(string $queueName)
    {
        if (!isset($this->queues[$queueName])) {
            throw new RuntimeException("Queue {$queueName} is not configured. Delete is aborted.");
        }
        $this->getChannel()->queue_delete($this->getQueueName($queueName));
        if ($queueName === '') {
            $this->generatedQueueName = null;
        }
        unset($this->queuesDeclared[$queueName], $this->ownedExclusiveQueues[$queueName]);
        // Losing a final binding can also remove an auto-delete exchange.
        $this->exchangesDeclared = [];
        $this->isDeclared = false;
    }

    /**
     * Delete the queue
     * @param string $exchangeName
     * @throws RuntimeException
     */
    public function deleteExchange(string $exchangeName)
    {
        if (!isset($this->exchanges[$exchangeName])) {
            throw new RuntimeException("Exchange {$exchangeName} is not configured. Delete is aborted.");
        }
        $this->getChannel()->exchange_delete($exchangeName);
        $this->exchangesDeclared = [];
        $this->isDeclared = false;
    }

    /**
     * Checks whether exchange is already declared in broker
     * @param string $exchangeName
     * @return bool
     */
    public function isExchangeExists(string $exchangeName) : bool
    {
        if ($exchangeName === '') {
            // RabbitMQ creates the default exchange on every vhost and forbids even passive declarations.
            return true;
        }
        try {
            $this->getChannel()->exchange_declare($exchangeName, null, true);
        } catch (AMQPProtocolChannelException $e) {
            $this->replaceClosedChannel();
            if ($e->getCode() !== 404) {
                throw $e;
            }
            return false;
        }

        return true;
    }

    /**
     * Checks whether queue is already declared in broker
     * @param string $queueName
     * @return bool
     */
    public function isQueueExists(string $queueName) : bool
    {
        try {
            $this->getChannel()->queue_declare($this->getQueueName($queueName), true);
        } catch (AMQPProtocolChannelException $e) {
            $this->replaceClosedChannel();
            if ($e->getCode() !== 404) {
                throw $e;
            }
            return false;
        }

        return true;
    }

    private function replaceClosedChannel()
    {
        // A protocol channel exception leaves the connection and its other resources intact.
        if ($this->conn->isConnected()) {
            $this->ch = $this->conn->channel();
        } else {
            $this->reset();
        }
    }

    /**
     * @param array $unnamedArr
     * @return array
     */
    private function arrangeByName(array $unnamedArr) : array
    {
        $namedArr = [];
        foreach ($unnamedArr as $elem) {
            if('' === $elem['name']) {
                $namedArr[$elem['name']][] = $elem;
            } else {
                $namedArr[$elem['name']] = $elem;
            }
        }

        return $namedArr;
    }

    /** Discard broker-session state after a disconnect. */
    public function reset()
    {
        $this->ch = null;
        $this->isDeclared = false;
        $this->generatedQueueName = null;
        $this->queuesDeclared = [];
        $this->exchangesDeclared = [];
    }

    /**
     * @return AMQPChannel
     */
    private function getChannel()
    {
        if (!$this->hasCurrentChannel()) {
            $this->reset();
            $this->ch = $this->conn->channel();
        }

        return $this->ch;
    }
}
