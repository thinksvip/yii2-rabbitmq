<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\helpers;

use DomainException;
use InvalidArgumentException;
use PhpAmqpLib\Wire\AMQPBufferReader;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Wire\AMQPWriter;

/** JSON storage for replay options, preserving AMQP header types and binary strings. */
final class ReplayOptionsCodec
{
    private const HEADERS_ENCODING = 'amqp-table-base64-v1';

    public static function encode(array $options): string
    {
        $properties = $options['additionalProperties'];
        // Match Producer::publish(): non-empty headers override application_headers.
        $headers = !empty($options['headers'])
            ? new AMQPTable($options['headers'])
            : ($properties['application_headers'] ?? null);
        if ($headers !== null) {
            if (!$headers instanceof AMQPTable && !is_array($headers)) {
                throw new InvalidArgumentException('application_headers must be an AMQPTable or a typed AMQP field array.');
            }
            $writer = new AMQPWriter();
            $writer->write_table($headers);
            $options['headers'] = base64_encode($writer->getvalue());
            $options['headers_encoding'] = self::HEADERS_ENCODING;
            $options['headers_protocol'] = AMQPTable::getProtocol();
            unset($options['additionalProperties']['application_headers']);
        }

        self::validateJsonValue($options);
        return json_encode($options, JSON_THROW_ON_ERROR | JSON_PRESERVE_ZERO_FRACTION);
    }

    /** Accept both JSON strings and native JSON/JSONB columns, including legacy rows. */
    public static function decode($stored): array
    {
        $options = is_string($stored) ? json_decode($stored, true, 512, JSON_THROW_ON_ERROR) : $stored;
        if (!is_array($options)) {
            throw new DomainException('Stored replay options must be an object.');
        }
        if (array_key_exists('headers_encoding', $options)) {
            if ($options['headers_encoding'] !== self::HEADERS_ENCODING
                || ($options['headers_protocol'] ?? null) !== AMQPTable::getProtocol()) {
                throw new DomainException('Unsupported stored AMQP headers encoding or protocol.');
            }
            $encoded = $options['headers'] ?? null;
            $wire = is_string($encoded) ? base64_decode($encoded, true) : false;
            if ($wire === false) {
                throw new DomainException('Stored AMQP headers are not valid base64.');
            }
            // A field table starts with its unsigned 32-bit payload length.
            $length = strlen($wire) >= 4 ? unpack('Nlength', substr($wire, 0, 4))['length'] : null;
            if ($length !== strlen($wire) - 4) {
                throw new DomainException('Stored AMQP headers have an invalid length.');
            }
            $reader = new AMQPBufferReader($wire);
            $headers = $reader->read_table_object();
            $options['additionalProperties']['application_headers'] = $headers;
            // Passing the table through Producer's native headers array would infer types again.
            $options['headers'] = null;
        }

        return $options;
    }

    private static function validateJsonValue($value, int $depth = 0): void
    {
        if ($depth > 64) {
            throw new InvalidArgumentException('Replay options are recursive or nested too deeply.');
        }
        if (is_array($value)) {
            foreach ($value as $entry) {
                self::validateJsonValue($entry, $depth + 1);
            }
        } elseif (!is_scalar($value) && $value !== null) {
            throw new InvalidArgumentException('Replay options contain an unsupported value of type ' . gettype($value) . '.');
        }
    }
}
