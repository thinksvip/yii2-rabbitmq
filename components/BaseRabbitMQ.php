<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\components;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Exception\AMQPChannelClosedException;
use PhpAmqpLib\Exception\AMQPConnectionClosedException;
use PhpAmqpLib\Exception\AMQPDataReadException;
use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPIOWaitException;
use PhpAmqpLib\Exception\AMQPSocketException;
use PhpAmqpLib\Exception\AMQPTimeoutException;

abstract class BaseRabbitMQ
{
    use ChannelCache;

    protected $autoDeclare;

    /**
     * @var $logger Logger
     */
    protected $logger;

    /**
     * @var $routing Routing
     */
    protected $routing;

    /**
     * @param AbstractConnection $conn
     * @param Routing $routing
     * @param Logger $logger
     * @param bool $autoDeclare
     */
    public function __construct(AbstractConnection $conn, Routing $routing, Logger $logger, bool $autoDeclare)
    {
        $this->conn = $conn;
        $this->routing = $routing;
        $this->logger = $logger;
        $this->autoDeclare = $autoDeclare;
        if ($conn->connectOnConstruct()) {
            $this->getChannel();
        }
    }

    public function __destruct()
    {
        // The DI container owns the connection and shares it with other services.
        $this->closeChannel();
    }

    protected function closeChannel()
    {
        if ($this->hasCurrentChannel()) {
            try {
                $this->ch->close();
            } catch (\Exception $e) {
                // ignore on shutdown
            }
        }
        $this->ch = null;
    }

    public function close()
    {
        $this->closeChannel();
        $this->routing->reset();
        if ($this->conn->isConnected()) {
            try {
                $this->conn->close();
            } catch (\Exception $e) {
                // ignore on shutdown
            }
        }
    }

    public function renew()
    {
        $this->ch = null;
        $this->routing->reset();
        $this->conn->reconnect();
    }

    /**
     * @return AMQPChannel
     */
    public function getChannel()
    {
        if (!$this->hasCurrentChannel()) {
            $this->ch = $this->conn->channel();
        }

        return $this->ch;
    }

    protected function isTransportFailure(\Throwable $error): bool
    {
        return $error instanceof AMQPConnectionClosedException
            || $error instanceof AMQPChannelClosedException
            || $error instanceof AMQPIOException
            || $error instanceof AMQPIOWaitException
            || $error instanceof AMQPSocketException
            || $error instanceof AMQPDataReadException
            || $error instanceof AMQPTimeoutException;
    }
}
