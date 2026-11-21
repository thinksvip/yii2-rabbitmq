<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\components;

use PhpAmqpLib\Connection\AbstractConnection;

/** Sends heartbeats while a CLI consumer is processing a delivery. */
final class ConsumerHeartbeat
{
    private $connection;
    private $interval;
    private $previousHandler;
    private $previousAsync;
    private $failure;

    private function __construct(AbstractConnection $connection, int $interval, int $previousHandler)
    {
        $this->connection = $connection;
        $this->interval = $interval;
        $this->previousHandler = $previousHandler;
    }

    public static function start(AbstractConnection $connection): ?self
    {
        $timeout = (int)$connection->getHeartbeat();
        if ($timeout <= 0 || PHP_SAPI !== 'cli' || (defined('AMQP_WITHOUT_SIGNALS') && AMQP_WITHOUT_SIGNALS)
            || !function_exists('pcntl_alarm') || !function_exists('pcntl_async_signals')
            || !function_exists('pcntl_signal') || !function_exists('pcntl_signal_get_handler')
            || !function_exists('pcntl_sigprocmask')) {
            return null;
        }

        // SIGALRM and the alarm timer are process-wide. Leave an application's timer in place.
        $previousHandler = pcntl_signal_get_handler(SIGALRM);
        if (!in_array($previousHandler, [SIG_DFL, SIG_IGN], true)
            || !pcntl_sigprocmask(SIG_BLOCK, [SIGALRM], $blocked) || in_array(SIGALRM, $blocked, true)) {
            return null;
        }
        try {
            $remaining = pcntl_alarm(0);
            if ($remaining > 0) {
                pcntl_alarm($remaining);
                return null;
            }

            $sender = new self($connection, (int)ceil($timeout / 2), $previousHandler);
            if (!pcntl_signal(SIGALRM, [$sender, 'tick'])) {
                return null;
            }
            $sender->previousAsync = pcntl_async_signals(true);
            pcntl_alarm($sender->interval);
            return $sender;
        } finally {
            pcntl_sigprocmask(SIG_UNBLOCK, [SIGALRM]);
        }
    }

    public function tick(): void
    {
        try {
            if ($this->connection->isConnected() && !$this->connection->isWriting()) {
                $this->connection->checkHeartBeat();
            }
        } catch (\Exception $e) {
            // Let the callback finish. Report the transport failure on the consumer's normal recovery path.
            $this->failure = $e;
            return;
        }
        pcntl_alarm($this->interval);
    }

    public function throwIfFailed(): void
    {
        if ($this->failure !== null) {
            throw $this->failure;
        }
    }

    public function stop(): void
    {
        if (pcntl_signal_get_handler(SIGALRM) === [$this, 'tick']) {
            // A callback may have blocked SIGALRM while the timer was running. Discard any pending tick.
            pcntl_signal(SIGALRM, SIG_IGN);
            pcntl_alarm(0);
            pcntl_signal(SIGALRM, $this->previousHandler);
        }
        pcntl_async_signals($this->previousAsync);
    }
}
