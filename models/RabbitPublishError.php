<?php

namespace mikemadisonweb\rabbitmq\models;

use DomainException;
use Exception;
use InvalidArgumentException;
use mikemadisonweb\rabbitmq\helpers\ReplayOptionsCodec;
use Yii;
use yii\behaviors\TimestampBehavior;
use yii\db\ActiveRecord;
use yii\db\Expression;
use yii\di\Instance;
use yii\mutex\FileMutex;
use yii\mutex\Mutex;

/**
 * This is the model class for table "rabbit_publish_error".
 *
 * @property int $id
 * @property string $message
 * @property int|null $created_at
 * @property int|null $updated_at
 * @property string $options
 * @property string|null $error
 * @property int|null $counter
 */
class RabbitPublishError extends ActiveRecord
{
    public const MUTEX_COMPONENT = 'rabbitmqReplayMutex';
    public const MUTEX_NAME = 'rabbit_publish_error';

    /**
     * @var Mutex|string|array|null Mutex instance, component ID or configuration.
     * Defaults to the rabbitmqReplayMutex application component (FileMutex on one host).
     * Configure a shared database mutex when replay runs on multiple hosts.
     */
    public $mutex;

    /** @var string */
    public $msgBody;
    /** @var string */
    public $exchangeName;
    /** @var string */
    public $routingKey = '';
    /** @var array */
    public $additionalProperties = [];
    /** @var array|null */
    public $headers = null;
    /** @var string */
    public $producerName;
    /** @var string */
    public $errorMsg;

    public static function tableName()
    {
        return 'rabbit_publish_error';
    }

    public function behaviors()
    {
        return [
            TimestampBehavior::class,
        ];
    }

    public function rules()
    {
        return [
            ['message', 'required', 'strict' => true],
            ['options', 'required'],
            [['message', 'error'], 'string'],
            [['created_at', 'updated_at', 'counter'], 'integer'],
            [['options'], 'safe'],
        ];
    }

    public function saveItem()
    {
        if ($this->producerName === null || $this->producerName === '') {
            throw new InvalidArgumentException('Field producerName is required!');
        }
        if ($this->msgBody === null) {
            throw new InvalidArgumentException('Field msgBody is required!');
        }
        if ($this->exchangeName === null) {
            throw new InvalidArgumentException('Field exchangeName is required!');
        }
        if ($this->errorMsg === null || $this->errorMsg === '') {
            throw new InvalidArgumentException('Field errorMsg is required!');
        }
        $this->message = $this->msgBody;
        $options = [
            'exchangeName' => $this->exchangeName,
            'producerName' => $this->producerName,
            'routingKey' => $this->routingKey,
            'additionalProperties' => $this->additionalProperties,
            'headers' => $this->headers
        ];
        $this->options = ReplayOptionsCodec::encode($options);
        $this->error = $this->errorMsg;
        $this->counter = 1;
        if (!$this->save()) {
            throw new DomainException(implode(' ', $this->getFirstErrors()));
        }
    }

    public function rePublish()
    {
        $mutex = $this->getReplayMutex();
        if (!$mutex->acquire(self::MUTEX_NAME)) {
            return;
        }
        try {
            foreach (self::find()->each() as $model) {
                /** @var self $model */
                try {
                    $options = ReplayOptionsCodec::decode($model->options);
                    if (!isset($options['producerName'], $options['exchangeName'])) {
                        throw new DomainException('Stored producerName and exchangeName are required.');
                    }
                    $producer = Yii::$app->rabbitmq->getProducer($options['producerName']);
                    $producer->publish(
                        $model->message,
                        $options['exchangeName'],
                        $options['routingKey'] ?? '',
                        $options['additionalProperties'] ?? [],
                        $options['headers'] ?? null
                    );
                    $model->delete();
                } catch (Exception $e) {
                    $model->error = $e->getMessage();
                    $model->counter = new Expression('COALESCE([[counter]], 0) + 1');
                    $model->save(false, ['error', 'counter', 'updated_at']);
                }
            }
        } finally {
            $mutex->release(self::MUTEX_NAME);
        }
    }

    protected function getReplayMutex(): Mutex
    {
        if ($this->mutex !== null) {
            return $this->mutex = Instance::ensure($this->mutex, Mutex::class);
        }
        if (!Yii::$app->has(self::MUTEX_COMPONENT)) {
            Yii::$app->set(self::MUTEX_COMPONENT, ['class' => FileMutex::class]);
        }

        return Instance::ensure(self::MUTEX_COMPONENT, Mutex::class);
    }
}
