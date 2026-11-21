<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests;

use mikemadisonweb\rabbitmq\DependencyInjection;
use Yii;
use yii\helpers\ArrayHelper;

abstract class TestCase extends \PHPUnit\Framework\TestCase
{
    public static $params;

    /**
     * Clean up after test.
     * By default the application created with [[mockApplication]] will be destroyed.
     */
    protected function tearDown(): void
    {
        parent::tearDown();
        $this->destroyApplication();
        Yii::$container = new \yii\di\Container();
    }

    /**
     * Populates Yii::$app with a new application
     * The application will be destroyed on tearDown() automatically.
     * @param array $config The application configuration, if needed
     * @param string $appClass name of the application class to create
     */
    protected function mockApplication($config = [], $appClass = '\yii\console\Application')
    {
        new $appClass(ArrayHelper::merge([
            'id' => 'testapp',
            'basePath' => __DIR__,
            'vendorPath' => $this->getVendorPath(),
        ], $config));
    }

    protected function getVendorPath()
    {
        return dirname(__DIR__) . '/vendor';
    }

    /**
     * Destroys application in Yii::$app by setting it to null.
     */
    protected function destroyApplication()
    {
        if (\Yii::$app && \Yii::$app->has('session', true)) {
            \Yii::$app->session->close();
        }
        \Yii::$app = null;
    }

    /**
     * Invokes a inaccessible method.
     * @param $object
     * @param $method
     * @param array $args
     * @return mixed
     * @since 2.0.11
     */
    protected function invokeMethod($object, $method, $args = [])
    {
        $reflection = new \ReflectionObject($object);
        $method = $reflection->getMethod($method);
        if (PHP_VERSION_ID < 80100) {
            $method->setAccessible(true);
        }
        return $method->invokeArgs($object, $args);
    }

    /**
     * Sets an inaccessible object property to a designated value.
     * @param $object
     * @param $propertyName
     * @param $value
     * @since 2.0.11
     */
    protected function setInaccessibleProperty($object, $propertyName, $value)
    {
        $class = new \ReflectionClass($object);
        while (!$class->hasProperty($propertyName)) {
            $class = $class->getParentClass();
        }
        $property = $class->getProperty($propertyName);
        if (PHP_VERSION_ID < 80100) {
            $property->setAccessible(true);
        }
        $property->setValue($object, $value);
    }

    /**
     * Gets an inaccessible object property.
     * @param $object
     * @param $propertyName
     * @return mixed
     */
    protected function getInaccessibleProperty($object, $propertyName)
    {
        $class = new \ReflectionClass($object);
        while (!$class->hasProperty($propertyName)) {
            $class = $class->getParentClass();
        }
        $property = $class->getProperty($propertyName);
        if (PHP_VERSION_ID < 80100) {
            $property->setAccessible(true);
        }
        return $property->getValue($object);
    }

    /**
     * Load extension to test app instance
     * @param array $config
     * @throws \mikemadisonweb\rabbitmq\exceptions\InvalidConfigException
     */
    protected function loadExtension(array $config)
    {
        $this->mockApplication($config);
        $di = new DependencyInjection();
        $di->bootstrap(\Yii::$app);
    }
}
