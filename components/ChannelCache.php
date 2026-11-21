<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\components;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;

trait ChannelCache
{
    /** @var AbstractConnection */
    protected $conn;

    /** @var AMQPChannel|null */
    protected $ch;

    protected function hasCurrentChannel(): bool
    {
        if ($this->ch === null || !$this->conn->isConnected()) {
            return false;
        }

        // Another service sharing the connection may have reconnected and reused this channel ID.
        $id = $this->ch->getChannelId();
        return $id !== null && ($this->conn->channels[$id] ?? null) === $this->ch;
    }
}
