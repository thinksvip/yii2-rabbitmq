<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\models;

use mikemadisonweb\rabbitmq\components\Producer;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\migrations\M201026132305RabbitPublishError;
use mikemadisonweb\rabbitmq\models\RabbitPublishError;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Wire\AMQPWriter;
use yii\caching\ArrayCache;
use yii\db\Connection;

/** @requires extension pdo_sqlite */
class RabbitPublishErrorTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->mockApplication(['components' => [
            'db' => ['class' => Connection::class, 'dsn' => 'sqlite::memory:'],
            'cache' => ['class' => ArrayCache::class],
            'rabbitmq' => ['class' => Configuration::class],
        ]]);
        $migration = new M201026132305RabbitPublishError(['compact' => true]);
        $migration->up();
    }

    private function newItem(): RabbitPublishError
    {
        return new RabbitPublishError([
            'msgBody' => 'message', 'exchangeName' => 'events', 'routingKey' => 'jobs',
            'producerName' => 'worker', 'additionalProperties' => ['content_type' => 'text/plain'],
            'headers' => ['retry' => true], 'errorMsg' => 'initial failure',
        ]);
    }

    public function testLegacyReplayOptionsRemainReadable()
    {
        $item = $this->newItem();
        $item->saveItem();
        RabbitPublishError::updateAll(['options' => json_encode([
            'producerName' => 'worker', 'exchangeName' => 'events', 'routingKey' => 'jobs',
            'additionalProperties' => ['content_type' => 'text/plain'], 'headers' => ['retry' => true],
        ])], ['id' => $item->id]);
        $producer = $this->createMock(Producer::class);
        $producer->expects($this->once())->method('publish')
            ->with('message', 'events', 'jobs', ['content_type' => 'text/plain'], ['retry' => true]);
        \Yii::$container->setSingleton(sprintf(Configuration::PRODUCER_SERVICE_NAME, 'worker'), $producer);
        $item->rePublish();
        $this->assertFalse(RabbitPublishError::find()->exists());
    }

    public function testTableHeadersRetainTheirTypesThroughDatabaseReplay()
    {
        $headers = new AMQPTable();
        $headers->set('small_integer', 42, AMQPTable::T_INT_SHORTSHORT);
        $headers->set('binary', "\0\xff", AMQPTable::T_BYTES);
        $writer = new AMQPWriter();
        $expectedWire = $writer->write_table($headers)->getvalue();
        $item = $this->newItem();
        $item->headers = null;
        $item->additionalProperties['application_headers'] = $headers;
        $item->saveItem();
        $producer = $this->createMock(Producer::class);
        $producer->expects($this->once())->method('publish')->with(
            'message', 'events', 'jobs', $this->callback(static function ($properties) use ($expectedWire) {
                $writer = new AMQPWriter();
                return $writer->write_table($properties['application_headers'])->getvalue() === $expectedWire;
            }), null
        );
        \Yii::$container->setSingleton(sprintf(Configuration::PRODUCER_SERVICE_NAME, 'worker'), $producer);
        $item->rePublish();
        $this->assertFalse(RabbitPublishError::find()->exists());
    }
}
