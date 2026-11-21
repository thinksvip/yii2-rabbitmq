<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\controllers;

use mikemadisonweb\rabbitmq\components\Consumer;
use mikemadisonweb\rabbitmq\components\Routing;
use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\controllers\RabbitMQController;
use mikemadisonweb\rabbitmq\tests\TestCase;
use yii\base\InvalidConfigException;
use yii\console\Controller;

class RabbitMQControllerTest extends TestCase
{
    protected $controller;

    public function setUp(): void
    {
        $this->loadExtension(
            [
                'components' => [
                    'rabbitmq' => [
                        'class'       => Configuration::class,
                        'connections' => [
                            [
                                'host' => 'unreal',
                            ],
                        ],
                    ],
                ],
            ]
        );
        $this->controller = $this->getMockBuilder(RabbitMQController::class)
            ->setConstructorArgs([Configuration::EXTENSION_CONTROLLER_ALIAS, \Yii::$app])
            ->onlyMethods(['stderr', 'stdout'])
            ->getMock();
    }

    /** @dataProvider unknownServices */
    public function testUnknownService($action, $arguments)
    {
        $this->expectException(InvalidConfigException::class);
        $this->controller->runAction($action, $arguments);
    }

    public static function unknownServices(): array
    {
        return [['consume', ['unknown']], ['publish', ['unknown', 'exchange']]];
    }

    /**
     * The options set process-wide AMQP constants in beforeAction.
     * @runInSeparateProcess
     * @preserveGlobalState disabled
     */
    public function testConsumeAliasesAndExitCode()
    {
        $consumer = $this->createMock(Consumer::class);
        $consumer->expects($this->once())->method('setMemoryLimit')->with(128);
        $consumer->expects($this->once())->method('consume')->with(4)->willReturn(17);
        \Yii::$container->setSingleton(sprintf(Configuration::CONSUMER_SERVICE_NAME, 'worker'), $consumer);
        $result = $this->controller->runAction('consume', [
            'worker', '_aliases' => ['l' => '128', 'm' => '4', 'w' => '1', 'd' => 'false'],
        ]);
        $this->assertTrue(AMQP_WITHOUT_SIGNALS);
        $this->assertFalse(AMQP_DEBUG);
        $this->assertSame(17, $result);
    }

    /** @dataProvider invalidLimits */
    public function testInvalidLimit($limits)
    {
        $consumer = $this->createMock(Consumer::class);
        $consumer->expects($this->never())->method('consume');
        \Yii::$container->setSingleton(sprintf(Configuration::CONSUMER_SERVICE_NAME, 'worker'), $consumer);
        $this->expectException(\InvalidArgumentException::class);
        $this->controller->runAction('consume', array_merge(['worker'], $limits));
    }

    public static function invalidLimits(): array
    {
        return [[['messagesLimit' => 'abc']], [['memoryLimit' => '-1']], [['_aliases' => ['m' => '1.5']]]];
    }

    public function testRestartUsesFreshConnectionAndSubscribesOnce()
    {
        $consumer = $this->createMock(Consumer::class);
        $consumer->expects($this->once())->method('renew');
        $consumer->expects($this->once())->method('consume')->with(0)->willReturn(19);
        $config = $this->createMock(Configuration::class);
        $config->expects($this->once())->method('getConsumer')->with('worker')->willReturn($consumer);
        \Yii::$app->set('rabbitmq', $config);
        $controller = new RabbitMQController('rabbitmq', \Yii::$app);
        $this->assertSame(19, $controller->runAction('restart-consume', ['worker']));
    }

    /** @requires extension pcntl */
    public function testDisabledSignalFunctionExplainsStartupFailure()
    {
        $process = proc_open([
            PHP_BINARY, '-d', 'disable_functions=pcntl_signal', __DIR__ . '/console-fixture.php',
            'rabbitmq/consume', 'local',
        ], [['pipe', 'r'], ['pipe', 'w'], ['pipe', 'w']], $pipes);
        $this->assertIsResource($process);
        fclose($pipes[0]);
        $out = stream_get_contents($pipes[1]);
        $err = stream_get_contents($pipes[2]);
        fclose($pipes[1]);
        fclose($pipes[2]);
        $this->assertNotSame(0, proc_close($process));
        $this->assertStringContainsString("Function 'pcntl_signal' is referenced", $out . $err);
    }

    public function testDeclareAllAction()
    {
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->exactly(2))
            ->method('declareAll')
            ->willReturnOnConsecutiveCalls(false, true);
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, $routing);
        $response = $this->controller->runAction('declare-all');
        $this->assertSame(Controller::EXIT_CODE_ERROR, $response);
        $response = $this->controller->runAction('declare-all');
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $response);
    }

    public function testDeclareQueueAction()
    {
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->exactly(2))
            ->method('isQueueExists')
            ->willReturnOnConsecutiveCalls(true, false);
        $routing->expects($this->once())
            ->method('declareQueue');
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, $routing);
        $response = $this->controller->runAction('declare-queue', ['queue-name']);
        $this->assertSame(Controller::EXIT_CODE_ERROR, $response);
        $response = $this->controller->runAction('declare-queue', ['queue-name']);
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $response);
    }

    public function testDeclareServerNamedQueueWithoutProbingAnUnknownName()
    {
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->never())->method('isQueueExists');
        $routing->expects($this->once())->method('declareQueue')->with('');
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, $routing);
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $this->controller->runAction('declare-queue', ['']));
    }

    public function testDeclareExchangeAction()
    {
        $routing = $this->createMock(Routing::class);
        $routing->expects($this->exactly(2))
            ->method('isExchangeExists')
            ->willReturnOnConsecutiveCalls(true, false);
        $routing->expects($this->once())
            ->method('declareExchange');
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, $routing);
        $response = $this->controller->runAction('declare-exchange', ['exchange-name']);
        $this->assertSame(Controller::EXIT_CODE_ERROR, $response);
        $response = $this->controller->runAction('declare-exchange', ['exchange-name']);
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $response);
    }

    public function testDeleteAllAction()
    {
        $this->controller->interactive = false;
        $routing                       = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('deleteAll')
            ->willReturnOnConsecutiveCalls(false, true);
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, $routing);
        $response = $this->controller->runAction('delete-all');
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $response);
    }

    public function testDeleteQueueAction()
    {
        $this->controller->interactive = false;
        $routing                       = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('deleteQueue');
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, $routing);
        $response = $this->controller->runAction('delete-queue', ['queue-name']);
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $response);
    }

    public function testDeleteExchangeAction()
    {
        $this->controller->interactive = false;
        $routing                       = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('deleteExchange');
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, $routing);
        $response = $this->controller->runAction('delete-exchange', ['exchange-name']);
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $response);
    }

    public function testPurgeQueueAction()
    {
        $this->controller->interactive = false;
        $routing                       = $this->createMock(Routing::class);
        $routing->expects($this->once())
            ->method('purgeQueue');
        \Yii::$container->setSingleton(Configuration::ROUTING_SERVICE_NAME, $routing);
        $response = $this->controller->runAction('purge-queue', ['queue-name']);
        $this->assertSame(Controller::EXIT_CODE_NORMAL, $response);
    }
}
