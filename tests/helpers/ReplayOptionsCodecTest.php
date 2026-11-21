<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\helpers;

use mikemadisonweb\rabbitmq\helpers\ReplayOptionsCodec;
use PhpAmqpLib\Wire\AMQPArray;
use PhpAmqpLib\Wire\AMQPDecimal;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Wire\AMQPWriter;
use PHPUnit\Framework\TestCase;

class ReplayOptionsCodecTest extends TestCase
{
    private function options(array $properties = [], ?array $headers = null): array
    {
        return [
            'producerName' => 'sender', 'exchangeName' => '', 'routingKey' => 'jobs',
            'additionalProperties' => $properties, 'headers' => $headers,
        ];
    }

    private function wire(AMQPTable $table): string
    {
        $writer = new AMQPWriter();
        return $writer->write_table($table)->getvalue();
    }

    public function testTableRoundTripPreservesFieldTypesAndBinaryData()
    {
        $array = new AMQPArray();
        $array->push(1, AMQPTable::T_INT_SHORTSHORT);
        $array->push(new AMQPTable(['nested' => true]));
        $headers = new AMQPTable(['nested_array' => $array, 'void' => null]);
        $headers->set('short', 123, AMQPTable::T_INT_SHORT);
        $headers->set('longlong', 123, AMQPTable::T_INT_LONGLONG);
        $headers->set('decimal', new AMQPDecimal(1234, 2), AMQPTable::T_DECIMAL);
        $headers->set('timestamp', 1700000000, AMQPTable::T_TIMESTAMP);
        $headers->set('bytes', "\0\xff\x80", AMQPTable::T_BYTES);
        $headers->set('string', "\0\xff", AMQPTable::T_STRING_LONG);
        $stored = ReplayOptionsCodec::encode($this->options(['application_headers' => $headers, 'message_id' => 'stable']));
        foreach ([$stored, json_decode($stored, true)] as $databaseValue) {
            $decoded = ReplayOptionsCodec::decode($databaseValue);
            $this->assertSame($this->wire($headers), $this->wire($decoded['additionalProperties']['application_headers']));
            $this->assertSame('stable', $decoded['additionalProperties']['message_id']);
            $this->assertNull($decoded['headers']);
        }
    }

    public function testExplicitHeadersOverridePropertyHeadersAndPreserveNativeTypes()
    {
        $headers = ['date' => new \DateTimeImmutable('@1700000000'), 'decimal' => new AMQPDecimal(125, 2)];
        $options = $this->options(['application_headers' => new AMQPTable(['ignored' => true])], $headers);
        $decoded = ReplayOptionsCodec::decode(ReplayOptionsCodec::encode($options));
        $this->assertSame($this->wire(new AMQPTable($headers)), $this->wire($decoded['additionalProperties']['application_headers']));
    }

    public function testEmptyHeadersDoNotOverridePropertyHeaders()
    {
        $headers = new AMQPTable(['retained' => true]);
        $decoded = ReplayOptionsCodec::decode(ReplayOptionsCodec::encode($this->options(['application_headers' => $headers], [])));
        $this->assertSame($this->wire($headers), $this->wire($decoded['additionalProperties']['application_headers']));
    }

    public function testLegacyTypedFieldArraysRemainSupported()
    {
        $options = $this->options(['application_headers' => ['byte' => ['b', 1]]]);
        $decoded = ReplayOptionsCodec::decode(ReplayOptionsCodec::encode($options));
        $expected = new AMQPTable();
        $expected->set('byte', 1, AMQPTable::T_INT_SHORTSHORT);
        $this->assertSame($this->wire($expected), $this->wire($decoded['additionalProperties']['application_headers']));
    }

    public function testUnknownObjectsAreRejectedBeforeTheyBecomeEmptyJsonObjects()
    {
        $this->expectException(\InvalidArgumentException::class);
        ReplayOptionsCodec::encode($this->options(['unsupported' => new \stdClass()]));
    }

    public function testUnsupportedApplicationHeadersObjectIsRejected()
    {
        $this->expectException(\InvalidArgumentException::class);
        ReplayOptionsCodec::encode($this->options(['application_headers' => new \stdClass()]));
    }

    public function testFieldTypesUnsupportedByTheLibraryWriterAreRejected()
    {
        $headers = new AMQPTable();
        $headers->set('float', 1.25, AMQPTable::T_FLOAT);
        $this->expectException(\PhpAmqpLib\Exception\AMQPInvalidArgumentException::class);
        ReplayOptionsCodec::encode($this->options(['application_headers' => $headers]));
    }

    public function testCorruptWireEncodingIsRejected()
    {
        $stored = json_decode(ReplayOptionsCodec::encode($this->options([], ['valid' => true])), true);
        $stored['headers'] = base64_encode(base64_decode($stored['headers']) . 'trailing');
        $this->expectException(\DomainException::class);
        ReplayOptionsCodec::decode($stored);
    }

    public function testLegacyJsonAndNativeOptionsAreNotReinterpreted()
    {
        $options = $this->options(['message_id' => 'stable'], ['nested' => ['key' => true]]);
        $this->assertSame($options, ReplayOptionsCodec::decode($options));
        $this->assertSame($options, ReplayOptionsCodec::decode(json_encode($options)));
    }
}
