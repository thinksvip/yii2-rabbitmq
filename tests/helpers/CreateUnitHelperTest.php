<?php declare(strict_types=1);

namespace mikemadisonweb\rabbitmq\tests\helpers;

use mikemadisonweb\rabbitmq\Configuration;
use mikemadisonweb\rabbitmq\helpers\CreateUnitHelper;
use mikemadisonweb\rabbitmq\tests\TestCase;
use yii\helpers\FileHelper;

class CreateUnitHelperTest extends TestCase
{
    public function testGeneratesWorkersAndReloadsTheExistingProcess()
    {
        $this->loadExtension(['components' => ['rabbitmq' => [
            'class' => Configuration::class,
            'connections' => [['host' => 'localhost']],
            'queues' => [['name' => 'jobs']],
            'consumers' => [
                ['name' => 'batch', 'callbacks' => ['jobs' => 'handler'], 'systemd' => [
                    'workers' => 2, 'memory_limit' => 256, 'timeout_stop_sec' => 7200, 'restart_sec' => 10,
                ]],
                ['name' => 'default', 'callbacks' => ['jobs' => 'handler']],
            ],
        ]]]);
        $directory = sys_get_temp_dir() . '/yii2-rabbitmq-units-' . bin2hex(random_bytes(8));
        try {
            $helper = new CreateUnitHelper([
                'units_dir' => $directory, 'work_dir' => '/srv/jobs', 'user' => 'queue', 'group' => 'queue',
            ]);
            $helper->create();
            $unit = file_get_contents($directory . '/consumer_batch_1.service');
            $this->assertSame($unit, file_get_contents($directory . '/consumer_batch_2.service'));
            $this->assertStringContainsString('ExecStart=php /srv/jobs/yii rabbitmq/consume batch -l 256', $unit);
            $this->assertStringContainsString('ExecReload=/bin/kill -HUP $MAINPID', $unit);
            $this->assertStringContainsString('TimeoutStartSec=30', $unit);
            $this->assertStringContainsString('TimeoutStopSec=7200', $unit);
            $this->assertStringContainsString('RestartSec=10', $unit);
            $this->assertStringNotContainsString('TimeoutSec=', $unit);
            $this->assertStringNotContainsString('postgresql', $unit);
            $this->assertStringContainsString("User=queue\nGroup=queue", $unit);
            $defaultUnit = file_get_contents($directory . '/consumer_default_1.service');
            $this->assertStringNotContainsString('-l ', $defaultUnit);
            $this->assertStringContainsString('TimeoutStopSec=300', $defaultUnit);
            $this->assertStringContainsString('RestartSec=5', $defaultUnit);
            $this->assertCount(3, glob($directory . '/*.service'));
            $this->assertSame($helper->bash, file_get_contents($directory . '/exec.sh'));
            $this->assertTrue(is_executable($directory . '/exec.sh'));
        } finally {
            FileHelper::removeDirectory($directory);
        }
    }
}
