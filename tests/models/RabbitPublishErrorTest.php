<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\models;

use mikemadisonweb\rabbitmq\components\Producer;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\migrations\M201026132305RabbitPublishError;
use mikemadisonweb\rabbitmq\models\RabbitPublishError;
use mikemadisonweb\rabbitmq\tests\TestCase;
use PhpAmqpLib\Wire\AMQPTable;
use PhpAmqpLib\Wire\AMQPWriter;
use yii\db\Connection;
use yii\helpers\FileHelper;
use yii\mutex\FileMutex;

/** @requires extension pdo_sqlite */
class RabbitPublishErrorTest extends TestCase
{
    private $migration;
    private $mutexPath;

    protected function setUp(): void
    {
        parent::setUp();
        $this->mutexPath = sys_get_temp_dir() . '/yii-rabbitmq-replay-' . bin2hex(random_bytes(8));
        $this->mockApplication(['runtimePath' => $this->mutexPath . '/runtime', 'components' => [
            'db' => ['class' => Connection::class, 'dsn' => 'sqlite::memory:'],
            RabbitPublishError::MUTEX_COMPONENT => [
                'class' => FileMutex::class, 'mutexPath' => $this->mutexPath, 'autoRelease' => false,
            ],
            'rabbitmq' => ['class' => Configuration::class],
        ]]);
        $this->migration = new M201026132305RabbitPublishError(['compact' => true]);
        $this->migration->up();
    }

    protected function tearDown(): void
    {
        if (\Yii::$app && \Yii::$app->has(RabbitPublishError::MUTEX_COMPONENT, true)) {
            \Yii::$app->get(RabbitPublishError::MUTEX_COMPONENT)->release(RabbitPublishError::MUTEX_NAME);
        }
        parent::tearDown();
        FileHelper::removeDirectory($this->mutexPath);
    }

    public function testMigrationCanRollBack()
    {
        $db = \Yii::$app->db;
        $this->assertNotNull($db->schema->getTableSchema(RabbitPublishError::tableName(), true));
        $this->migration->down();
        $this->assertNull($db->schema->getTableSchema(RabbitPublishError::tableName(), true));
    }

    private function newItem(): RabbitPublishError
    {
        return new RabbitPublishError([
            'msgBody' => '0', 'exchangeName' => '', 'routingKey' => 'jobs',
            'producerName' => 'worker', 'additionalProperties' => ['content_type' => 'text/plain'],
            'headers' => ['retry' => true], 'errorMsg' => 'initial failure',
        ]);
    }

    public function testSuccessfulAndFailedReplay()
    {
        $item = $this->newItem();
        $item->saveItem();
        $producer = $this->createMock(Producer::class);
        \Yii::$container->setSingleton(sprintf(Configuration::PRODUCER_SERVICE_NAME, 'worker'), $producer);
        $producer->expects($this->exactly(2))->method('publish')
            ->with('0', '', 'jobs', $this->callback(static function ($properties) {
                return $properties['content_type'] === 'text/plain'
                    && $properties['application_headers'] instanceof AMQPTable
                    && $properties['application_headers']->getNativeData() === ['retry' => true];
            }), null)
            ->willReturnOnConsecutiveCalls($this->throwException(new \RuntimeException('unavailable')), null);
        $item->rePublish();
        $item->refresh();
        $this->assertSame(2, $item->counter);
        $this->assertSame('unavailable', $item->error);
        $this->assertGreaterThan(0, $item->created_at);
        $this->assertFalse(\Yii::$app->get(RabbitPublishError::MUTEX_COMPONENT)->isAcquired(RabbitPublishError::MUTEX_NAME));
        $item->rePublish();
        $this->assertFalse(RabbitPublishError::find()->exists());
    }

    public function testExistingLockPreventsReplay()
    {
        $item = $this->newItem();
        $item->saveItem();
        $mutex = \Yii::$app->get(RabbitPublishError::MUTEX_COMPONENT);
        $this->assertTrue($mutex->acquire(RabbitPublishError::MUTEX_NAME));
        $item->rePublish();
        $this->assertTrue($mutex->isAcquired(RabbitPublishError::MUTEX_NAME));
        $this->assertSame(1, RabbitPublishError::findOne($item->id)->counter);
    }

    public function testUnexpectedFailureReleasesLock()
    {
        $item = $this->newItem();
        $item->saveItem();
        $producer = $this->createMock(Producer::class);
        $producer->method('publish')->willThrowException(new \TypeError('application error'));
        \Yii::$container->setSingleton(sprintf(Configuration::PRODUCER_SERVICE_NAME, 'worker'), $producer);
        try {
            $item->rePublish();
            $this->fail('Expected the publishing error.');
        } catch (\TypeError $e) {
            $this->assertSame('application error', $e->getMessage());
        }
        $this->assertFalse(\Yii::$app->get(RabbitPublishError::MUTEX_COMPONENT)->isAcquired(RabbitPublishError::MUTEX_NAME));
        $this->assertTrue(RabbitPublishError::find()->exists());
    }

    public function testDefaultMutexDoesNotRequireACacheComponent()
    {
        \Yii::$app->set(RabbitPublishError::MUTEX_COMPONENT, null);
        $this->newItem()->rePublish();
        $this->assertInstanceOf(FileMutex::class, \Yii::$app->get(RabbitPublishError::MUTEX_COMPONENT));
        $this->assertFalse(\Yii::$app->has('cache'));
    }

    public function testMutexCanBeInjectedByComponentName()
    {
        $item = $this->newItem();
        $item->mutex = RabbitPublishError::MUTEX_COMPONENT;
        $item->rePublish();
        $this->assertSame(\Yii::$app->get(RabbitPublishError::MUTEX_COMPONENT), $item->mutex);
    }

    public function testKilledReplayReleasesItsLockForAnotherProcess()
    {
        if (DIRECTORY_SEPARATOR === '\\' || !function_exists('proc_open')) {
            $this->markTestSkipped('Requires POSIX process termination and proc_open.');
        }
        $item = $this->newItem();
        $item->saveItem();
        $producer = $this->createMock(Producer::class);
        $producer->expects($this->once())->method('publish');
        \Yii::$container->setSingleton(sprintf(Configuration::PRODUCER_SERVICE_NAME, 'worker'), $producer);
        $process = proc_open([PHP_BINARY, __DIR__ . '/replay-lock-fixture.php', $this->mutexPath], [
            ['pipe', 'r'], ['pipe', 'w'], ['pipe', 'w'],
        ], $pipes);
        $this->assertIsResource($process);
        try {
            stream_set_timeout($pipes[1], 10);
            $this->assertSame("locked\n", fgets($pipes[1]));
            $item->rePublish();
            $this->assertTrue(RabbitPublishError::find()->exists());
            // SIGKILL skips both finally and shutdown handlers; flock releases via the OS.
            $this->assertTrue(proc_terminate($process, 9));
            proc_close($process);
            $process = null;
            $item->rePublish();
            $this->assertFalse(RabbitPublishError::find()->exists());
        } finally {
            if (is_resource($process)) {
                proc_terminate($process, 9);
                proc_close($process);
            }
            foreach ($pipes as $pipe) {
                if (is_resource($pipe)) {
                    fclose($pipe);
                }
            }
        }
    }

    public function testLegacyReplayOptionsRemainReadable()
    {
        $item = $this->newItem();
        $item->saveItem();
        RabbitPublishError::updateAll(['options' => json_encode([
            'producerName' => 'worker', 'exchangeName' => '', 'routingKey' => 'jobs',
            'additionalProperties' => ['content_type' => 'text/plain'], 'headers' => ['retry' => true],
        ])], ['id' => $item->id]);
        $producer = $this->createMock(Producer::class);
        $producer->expects($this->once())->method('publish')
            ->with('0', '', 'jobs', ['content_type' => 'text/plain'], ['retry' => true]);
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
            '0', '', 'jobs', $this->callback(static function ($properties) use ($expectedWire) {
                $writer = new AMQPWriter();
                return $writer->write_table($properties['application_headers'])->getvalue() === $expectedWire;
            }), null
        );
        \Yii::$container->setSingleton(sprintf(Configuration::PRODUCER_SERVICE_NAME, 'worker'), $producer);
        $item->rePublish();
        $this->assertFalse(RabbitPublishError::find()->exists());
    }

    public function testMalformedReplayEntryRemainsAvailableForRepair()
    {
        $item = $this->newItem();
        $item->saveItem();
        RabbitPublishError::updateAll(['options' => '{}'], ['id' => $item->id]);
        $item->rePublish();
        $item->refresh();
        $this->assertSame(2, $item->counter);
        $this->assertStringContainsString('Stored producerName and exchangeName', $item->error);
    }

    public function testValidationFailureReportsAnErrorWithoutPersisting()
    {
        $item = $this->newItem();
        $item->msgBody = ['not a persisted string'];
        try {
            $item->saveItem();
            $this->fail('Expected validation to reject the body.');
        } catch (\DomainException $e) {
            $this->assertNotSame('', $e->getMessage());
        }
        $this->assertFalse(RabbitPublishError::find()->exists());
    }

    /** @dataProvider requiredFields */
    public function testRequiredInput($field)
    {
        $item = $this->newItem();
        $item->$field = null;
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage($field);
        $item->saveItem();
    }

    public static function requiredFields(): array
    {
        return [['msgBody'], ['exchangeName'], ['producerName'], ['errorMsg']];
    }
}
