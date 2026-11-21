<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\components;

use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\exceptions\InvalidConfigException;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPLazySSLConnection;
use PhpAmqpLib\Connection\AMQPSocketConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class AbstractConnectionFactory
{
    /** @var string */
    private $_class;

    /** @var array */
    private $_parameters;

    /**
     * Constructor
     *
     * @param string $class      FQCN of AMQPConnection class to instantiate.
     * @param array  $parameters Map containing parameters resolved by Extension.
     */
    public function __construct($class, array $parameters)
    {
        $this->_class = $class;
        $this->_parameters = self::normalizeParameters($parameters);
    }

    public function createConnection() : AbstractConnection
    {
        $class = $this->_class;
        $options = $this->_parameters;
        $sslOptions = $options['ssl_context'];
        $locale = 'en_EN';
        if (is_a($class, AMQPSSLConnection::class, true)) {
            // Both legacy TLS constructors emit E_USER_DEPRECATED in php-amqplib 3.7.
            if ($class === AMQPSSLConnection::class || $class === AMQPLazySSLConnection::class) {
                $class = $class === AMQPSSLConnection::class ? AMQPStreamConnection::class : AMQPLazyConnection::class;
            }
            // The legacy constructor opens a plaintext stream when given an empty option array.
            if (!$sslOptions) {
                $sslOptions = ['crypto_method' => STREAM_CRYPTO_METHOD_TLS_CLIENT];
            }
            $locale = 'en_US';
        }
        $args = [$options['host'], $options['port'], $options['user'], $options['password'], $options['vhost']];
        $timeout = $options['read_write_timeout'];
        if (is_a($class, AMQPSocketConnection::class, true)) {
            if ($sslOptions !== null) {
                throw new \InvalidArgumentException('Socket connections do not support TLS options.');
            }
            // The socket constructor takes read timeout, keepalive, then write timeout.
            $io = [$timeout, $options['keepalive'], $timeout];
        } else {
            $context = null;
            if ($sslOptions !== null) {
                $constructor = new \ReflectionMethod($class, '__construct');
                if ($constructor->getDeclaringClass()->getName() !== AMQPStreamConnection::class) {
                    // Custom connection classes may use the legacy SSL constructor.
                    return new $class(...array_merge($args, [$sslOptions, [
                        'connection_timeout' => $options['connection_timeout'],
                        'read_write_timeout' => $timeout,
                        'keepalive' => $options['keepalive'],
                        'heartbeat' => $options['heartbeat'],
                        'channel_rpc_timeout' => $options['channel_rpc_timeout'],
                    ]]));
                }
                $context = stream_context_create(['ssl' => $sslOptions + ['crypto_method' => STREAM_CRYPTO_METHOD_TLS_CLIENT]]);
            }
            $io = [$options['connection_timeout'], $timeout, $context, $options['keepalive']];
        }

        return new $class(...array_merge(
            $args,
            [false, 'AMQPLAIN', null, $locale],
            $io,
            [$options['heartbeat'], $options['channel_rpc_timeout']]
        ));
    }

    /** Resolve URL overrides and validate the effective options before opening a connection. */
    public static function normalizeParameters(array $parameters): array
    {
        $parameters = array_replace(Configuration::DEFAULTS['connections'][0], $parameters);
        if ($parameters['url'] !== null) {
            $parameters = self::parseUrl($parameters);
        }
        foreach (['host', 'user', 'password', 'vhost'] as $key) {
            if (!is_string($parameters[$key]) || ($key === 'host' && trim($parameters[$key]) === '')) {
                throw self::invalidOption($parameters, $key, 'must be a string' . ($key === 'host' ? ' containing a host' : ''));
            }
        }
        foreach (['port' => [1, 65535], 'heartbeat' => [0, 65535]] as $key => [$min, $max]) {
            $value = $parameters[$key];
            if ((!is_int($value) && !(is_string($value) && preg_match('/^[0-9]+$/D', $value)))
                || $value < $min || $value > $max) {
                throw self::invalidOption($parameters, $key, "must be an integer between {$min} and {$max}");
            }
            $parameters[$key] = (int)$value;
        }
        foreach (['connection_timeout', 'read_write_timeout', 'channel_rpc_timeout'] as $key) {
            $value = $parameters[$key];
            if ((!is_int($value) && !is_float($value) && !(is_string($value) && is_numeric($value)))
                || !is_finite((float)$value) || $value < 0) {
                throw self::invalidOption($parameters, $key, 'must be a finite, non-negative number of seconds');
            }
            $parameters[$key] = $value + 0;
        }
        if ($parameters['channel_rpc_timeout'] > $parameters['read_write_timeout']) {
            throw self::invalidOption($parameters, 'channel_rpc_timeout', 'must not exceed read_write_timeout');
        }
        $keepalive = $parameters['keepalive'];
        if (is_string($keepalive)) {
            $keepalive = strtolower($keepalive);
        }
        if (in_array($keepalive, [true, 1, '1', 'true'], true)) {
            $parameters['keepalive'] = true;
        } elseif (in_array($keepalive, [false, 0, '0', 'false'], true)) {
            $parameters['keepalive'] = false;
        } else {
            throw self::invalidOption($parameters, 'keepalive', 'must be true, false, 1 or 0');
        }

        return $parameters;
    }

    private static function parseUrl(array $parameters): array
    {
        $value = $parameters['url'];
        if (!is_string($value) || $value === '' || preg_match('/%(?![0-9a-f]{2})/i', $value)) {
            throw self::invalidOption($parameters, 'url', 'must be a valid amqp URL');
        }
        $url = parse_url($value);
        if ($url === false || ($url['scheme'] ?? null) !== 'amqp' || empty($url['host']) || isset($url['fragment'])) {
            throw self::invalidOption($parameters, 'url', 'must be an amqp URL with a host and no fragment');
        }
        foreach (['host' => 'host', 'port' => 'port', 'user' => 'user', 'pass' => 'password'] as $part => $key) {
            if (isset($url[$part])) {
                $parameters[$key] = $part === 'port' ? $url[$part] : rawurldecode($url[$part]);
            }
        }
        if (isset($url['path'])) {
            $path = $url['path'];
            $parameters['vhost'] = rawurldecode(substr($path, 0, 1) === '/' ? substr($path, 1) : $path);
        }
        if (isset($url['query'])) {
            $allowed = ['host', 'port', 'user', 'password', 'vhost', 'connection_timeout',
                'read_write_timeout', 'keepalive', 'heartbeat', 'channel_rpc_timeout'];
            $seen = [];
            foreach (explode('&', $url['query']) as $pair) {
                if ($pair === '') {
                    continue;
                }
                [$key, $queryValue] = array_pad(explode('=', $pair, 2), 2, '');
                $key = urldecode($key);
                if (!in_array($key, $allowed, true)) {
                    // Do not include URL contents, credentials or unrecognised input in diagnostics.
                    throw self::invalidOption($parameters, 'url', 'contains an unknown query option');
                }
                if (isset($seen[$key])) {
                    throw self::invalidOption($parameters, $key, 'must not occur more than once in the URL query');
                }
                $parameters[$key] = urldecode($queryValue);
                $seen[$key] = true;
            }
        }
        $parameters['url'] = null;

        return $parameters;
    }

    private static function invalidOption(array $parameters, string $key, string $reason): InvalidConfigException
    {
        $name = is_string($parameters['name'] ?? null) ? $parameters['name'] : 'unnamed';
        return new InvalidConfigException("Connection `{$name}` option `{$key}` {$reason}.");
    }
}
